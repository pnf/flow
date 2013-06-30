package com.podsnap.flow
//import com.typesafe.scalalogging.slf4j.Logging
import grizzled.slf4j.Logging
import scala.collection.mutable.HashSet
import scala.collection.mutable.Set
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
  val edgeId2EdgePos = v.edges.map(new EdgeId(_)).zipWithIndex.toMap
}

@serializable
class Solver(g : Graph, n_iter:Int,
	     beta_cost: Double, beta_flow: Double, 
	     imbalance_gain : Double ) {

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


  
  def solve(sc: SparkContext) {

    // Wrap up vertices in a container that also holds linear algebra state
    val vertexCache = g.vertices.map(new VertexCache(_))

    // Parallelize the vertices on spark, and mark them as a persistent cache.
    val vertices = sc.parallelize(vertexCache).cache()

    // Broadcast the topology of the graph as read-only data
    val bg = sc.broadcast(g)

    for(i <- 1 to n_iter) {

      // Rebalance a single vertex and emit list of pairs [(EdgeId, EdgeVal), ...]
      def vertexToRebalancedEdges(vc:VertexCache) : Seq[(EdgeId,EdgeVal)] = {
        rebalanceFlows(vc);
        adjustCosts(vc);
        vc.v.edges.map(edgePair(_))
      }

      // Create list of all updated edges.
      // Since we rebalanced each vertex separately, and since each
      // edge touches two vertices, there will be duplicate EdgeIds
      // with different EdgeVals in this list.
      // 
      // So group the multiple edgevalues by edge id:
      val edgeGroups : spark.RDD[(EdgeId,Seq[EdgeVal])] =
        vertices.flatMap(vertexToRebalancedEdges).groupByKey()

      // Consolidate multiple updates for an edge into a single update
      def consolidateEdgeUpdates(evs:Seq[EdgeVal]) : EdgeVal = {
        var a=0.0; var b=0.0; var x=0.0;
        for(ev<-evs) {a+=ev.a; b+=ev.b; x+=ev.x}
        a/=evs.size; b/=evs.size; x/=evs.size
        new EdgeVal(a,b,x)
      }

      // Create list of consolidated, nondegenerate (EdgeId,EdgeVal) pairs
      val updatedEdges : spark.RDD[(EdgeId,EdgeVal)] =
        edgeGroups.map( p => (p._1,consolidateEdgeUpdates(p._2)))

      // Distribute the edge update pair out to the relevant vertices.
      // Note that we _must_ pass in the broadcast variabl bg, since it will
      // otherwise have null value remotely.
      def distributeEdgeUpdate(bg:spark.broadcast.Broadcast[Graph],
        eid:EdgeId, ev: EdgeVal) : Set[(Int,EdgeUpdate)] = {
        val eu = new EdgeUpdate(eid,ev)
        bg.value.edgeId2VertexIndices(eid).map( (_,eu) )
      }
      // Create ordered list of edge update sets for each vertex
      val vertexUpdatedEdges : spark.RDD[Seq[EdgeUpdate]] = 
        updatedEdges
          .flatMap( p => distributeEdgeUpdate(bg,p._1,p._2))  // Seq[(Int,EdgeUpdate)]
          .groupByKey()           // Seq[(Int,Seq[EdgeUpdate])]
          .sortByKey()
          .map(p => p._2)	  // Seq[Seq[EdgeUpdate]]



      def applyEdgeUpdatesToVertex(vc:VertexCache, eus:Seq[EdgeUpdate]) {
        eus.foreach( eu => {
          val i = vc.edgeId2EdgePos(eu.id)
          val e : Edge = vc.v.edges(i)
          e.a = eu.value.a
          e.b = eu.value.b
          e.x = eu.value.x
        })
      }
      // Apply updates to every vertex
      vertices
        .zip(vertexUpdatedEdges)
        .foreach( p => applyEdgeUpdatesToVertex(p._1,p._2))

    }

    val finalEdges : Seq[Edge] = vertices.flatMap(vc => vc.v.edges).map(e => (new EdgeId(e),e)).reduceByKey( (v1,v2) => v1).map(p => p._2).toArray()
    println(finalEdges.mkString("\n"))

  }

}

