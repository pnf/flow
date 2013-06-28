package com.podsnap.flow
//import com.typesafe.scalalogging.slf4j.Logging
import grizzled.slf4j.Logging
import scala.collection.mutable.HashSet
//import scala.math._
import breeze.linalg._
import breeze.numerics._
import spark.SparkContext
import spark.SparkContext._
import spark.AccumulatorParam
import scala.collection.TraversableOnce

object Solver {
  // Class to accumulate edge data
  @serializable class EdgeSet(val edges: HashSet[Edge]) {
  }
  // This crap is necessary to use accumulators
  implicit object EdgeSetAP extends AccumulatorParam[EdgeSet] {
    def zero(es: EdgeSet) = new EdgeSet(new HashSet[Edge]())
    def addInPlace(es1:EdgeSet, es2:EdgeSet) : EdgeSet = {
      es2.edges ++= es1.edges
      return es1
    }
  }

  def edgePair(e:Edge) : (EdgeId,EdgeVal) = {
    val id = new EdgeId(e)
    val value = new EdgeVal(e)
    return (id,value)
  }
}
import Solver._


@serializable class VertexCache(val v:Vertex) {
  val S : DenseMatrix[Double] = DenseMatrix.zeros[Double](v.edges.size,v.edges.size)
  val M : DenseMatrix[Double] = DenseMatrix.zeros[Double](v.edges.size,1)
  val S0 : DenseMatrix[Double] = DenseMatrix.zeros[Double](v.edges.size,v.edges.size)
  for(j<-0 until v.edges.size; k<-0 until v.edges.size) {
    S0(j,k) = v.direction(j)*v.direction(k)
  }
  val edgeId2Edge = v.edges.map(e => (new EdgeId(e), e)).toMap
}

@serializable
class Solver(g : Graph, n_iter:Int,
	     beta_cost: Double, beta_flow: Double, 
	     imbalance_gain : Double ) {

  val vertices = g.vertices.map(new VertexCache(_))
  val edges = g.edges

  def rebalanceFlows(v:VertexCache) {
    val i = v.v.i
    val d = v.v.direction
    // Matrix is a's on the diagonal, plus imbalance_gain everywhere
    v.S := v.S0*imbalance_gain
    val dd = DenseVector(v.v.edges.map(e => imbalance_gain + e.a))
    diag(v.S) := dd
    // rhs is a*b*direction
    v.M(::,0) := DenseVector(v.v.edges.zip(d).map(ed => ed._1.a*ed._1.b*ed._2))
    val X = v.S \ v.M
    //logger.trace(s"Rebalancing $v\n${v.M}\n${v.S}\n$X")
    //logger.trace("Rebalancing " + v + "\n" + v.M + "\n" + v.S + "\n" + X)
    for(i<-0 until v.v.edges.size) {
      v.v.edges(i).x = (1.0-beta_flow)*v.v.edges(i).x + beta_flow*X(i,0)
    }
  }

  def adjustCosts(v:VertexCache) {

    // Now rebalance costs.
    // For each edge, treat it as a source that is at target, i.e. b=x, a=big
    val a_source = 100.0
    val eps=0.1

    val d = v.v.direction
    // Matrix isa's on the diagonal, plus imbalance_gain everywhere
    v.S := v.S0*imbalance_gain
    val dd = DenseVector(v.v.edges.map(e => imbalance_gain + e.a))
    diag(v.S) := dd
    // rhs is a*b*direction
    v.M(::,0) := DenseVector(v.v.edges.zip(d).map(ed => ed._1.a*ed._1.b*ed._2))

    val i = v.v.i
    // Compute new coefficients a and b for each edge
    val coeffs = for( (e,j) <- v.v.edges.zipWithIndex if e.i>=0) yield {
      val m0 = v.M(j,0)
      val s0 = v.S(j,j)
      // Costs from other edges for x -> x +/- eps
      val c = for (dx <- List(-eps,0.0,eps) ) yield {
	v.M(j,0) = a_source * (e.x+dx)
	v.S(j,j)  = imbalance_gain + a_source
	val X = v.S \ v.M
	// compute cost for vertices other than j
	val otherCosts = for( (ek,k) <- v.v.edges.zipWithIndex if j!=k) yield {
	  ek.a * math.pow(X(k,0) - ek.b, 2)
	}
	otherCosts.sum
      }
      v.M(j,0) = m0
      v.S(j,j) = s0
      // Recompute a and b
      val dcdx = 2.0*e.a*(e.x-e.b) + (c(2)-c(0))/(2.0*eps)
      val d2cdx2 = 2.0*e.a + (c(0)-2*c(1)+c(2))/(eps*eps)
      val a = d2cdx2/2.0
      val b = e.x - dcdx/(2.0*a)
      //
      //logger.trace("Rebalanced costs " + i + " " + j + " " + e.a + " " + e.b + "  ==> " + a + " " + b)
      (e,a,b)
    }
    for ( (e,a,b) <- coeffs )  {
      // e.a = (1.0-beta_cost)*e.a + beta_cost*a
      e.b = (1.0-beta_cost)*e.b + beta_cost*b
    }
  }



  /*
   * Doing this remotely, each vertex is serialized and optimized separately.
   * For the next iteration, we need to share the edge updates.

   So, we want the 
       vertices.flatMap(rebalanceStuff)      ==> (i_edge, edgeUpdate),...
               .reduceByKey(...)             ==> (i_edge, consolidatedEdge)
               .flatMap(...)                 ==> (i_vertex, consolidatedEdge)
               .reduceByKey(...)             ==> vertices

   Will be helpful to sc.broadcast the map topology (sans matrices)
   And cache the part with matrices

   

   The

   */

  
  def solve(sc: SparkContext) {

    val vv = g.vertices.map(new VertexCache(_))
    val vvp = sc.parallelize(vv).cache()
    val bg = sc.broadcast(g)

    val edgePairs = vvp.flatMap(vc => {
      rebalanceFlows(vc);
      adjustCosts(vc);
      vc.v.edges.map(edgePair(_))
    })

    val edgeGroups = edgePairs.groupByKey()  // ==>    [ (EdgeID, [EdgeVal,...] ),...]

    val updatedEdges = edgeGroups.map( p => { //   ==> [ (EdgeID, EdgeVal),...]
      val eid=p._1; val evs=p._2
      var a=0.0; var b=0.0; var x=0.0;
      for(ev<-evs) {a+=ev.a; b+=ev.b; x+=ev.x}
      a/=evs.size; b/=evs.size; x/=evs.size
      (eid,new EdgeVal(a,b,x))
    })

    // [ [EdgeUpdate, ...]),...]
    val virtexUpdatedEdges : spark.RDD[Seq[EdgeUpdate]] = updatedEdges.flatMap( p => { 
      val eid=p._1; val ev=p._2
      val eu = new EdgeUpdate(eid,ev)
      bg.value.edgeId2VertexIndices(eid).map( (_,eu) )
    }).groupByKey().sortByKey().map(p => p._2)


    val bleh = vvp.zip(virtexUpdatedEdges)


    bleh.foreach( p => {
      val vc : VertexCache = p._1
      val eus : Seq[EdgeUpdate] = p._2
        eus.map( eu => {
          val e : Edge = vc.edgeId2Edge(eu.id)
          e.a = eu.value.a
          e.b = eu.value.b
          e.x = eu.value.x
      })
    })


    //val edges = g.edges

    //val distv = sc.parallelize(vertices)
    val distv = vertices

    for(i <- 1 to n_iter) {
      distv.foreach(rebalanceFlows)
      distv.foreach(adjustCosts)
      val cr = "\n"
      //logger.debug(s"Iteration $i:\n${edges.mkString(cr)}\n${vertices.mkString(cr)}")
      //logger.debug("Iteration " + i +"\n" + edges.mkString(cr) + "\n" + vertices.mkString(cr))
      //logger.debug(s"Total: target=${edges.map(e=>e.target).sum} flow=${edges.map(e=>e.x).sum} cost=${edges.map(e=>e.a*(e.x-e.b)*(e.x-e.b)).sum}")
    }
  }


}

