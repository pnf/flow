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

  // Class to accumulate edge data
  @serializable class EdgeSet(val edges: HashSet[Edge]) {
  }

@serializable class VertexCache(val v:Vertex) {
  val S : DenseMatrix[Double] = DenseMatrix.zeros[Double](v.edges.size,v.edges.size)
  val M : DenseMatrix[Double] = DenseMatrix.zeros[Double](v.edges.size,1)
  val S0 : DenseMatrix[Double] = DenseMatrix.zeros[Double](v.edges.size,v.edges.size)
  for(j<-0 until v.edges.size; k<-0 until v.edges.size) {
    S0(j,k) = v.direction(j)*v.direction(k)
  }
  val edgeId2EdgePos = v.edges.map(new EdgeId(_)).zipWithIndex.toMap
}

object Solver {
  // This crap is necessary to use accumulators
  implicit object EdgeSetAP extends AccumulatorParam[EdgeSet] {
    def zero(es: EdgeSet) = new EdgeSet(new HashSet[Edge]())
    def addInPlace(es1:EdgeSet, es2:EdgeSet) : EdgeSet = {
      es2.edges ++= es1.edges
      return es1
    }
  }
}

@serializable
class Solver(val g : Graph, val n_iter:Int,
  val beta_cost: Double, val beta_flow: Double,
  val imbalance_gain : Double ) {

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

    // Wrap up vertices in a container that also holds linear algebra state.
    // val vertexCache = g.vertices.map(new VertexCache(_))

    // Parallelize the vertices on spark, and mark them as a persistent cache.
    // val vertices = sc.parallelize(vertexCache).cache()

    var vertices : spark.RDD[(Int,VertexCache)] = 
      sc.parallelize(g.vertices.zipWithIndex.map(_.swap)).mapValues(new VertexCache(_))

    // Broadcast the topology of the graph as read-only data
    val bg = sc.broadcast(g)

    for(i <- 1 to n_iter) {

      // Rebalance a single vertex and emit list of pairs [(EdgeId, EdgeVal), ...]
      def vertexToRebalancedEdges(vc:VertexCache) : Seq[(EdgeId,EdgeVal)] = {
        rebalanceFlows(vc);
        adjustCosts(vc);
        vc.v.edges.map(e => (new EdgeId(e),new EdgeVal(e)))
      }

      // Create list of all updated edges.
      // Since we rebalanced each vertex separately, and since each
      // edge touches two vertices, there will be duplicate EdgeIds
      // with different EdgeVals in this list.
      // 
      // So group the multiple edgevalues by edge id:
      val edgeGroups : spark.RDD[(EdgeId,Seq[(Int,EdgeVal)])  ] =
        vertices.flatMapValues(vertexToRebalancedEdges)        // still indexed by i_vert
                .map {case (i,(eid,ev)) => (eid,(i,ev)) }      // [(eid,(i,v)]
                .groupByKey                                    // [(eid,[(i,v)])]

      def invert[A,B](sab:Seq[(A,B)]) : (Seq[A],Seq[B]) = {
        (sab.map(_._1),sab.map(_._2))  }

      // Consolidate multiple updates for an edge into a single update
      // [(i,ev)] => (ev,[i])
      def consolidateEdgeUpdates(evs:Seq[(Int,EdgeVal)]) : (EdgeVal,Seq[Int]) = {
        var a=0.0; var b=0.0; var x=0.0;
        for((i,ev)<-evs) {a+=ev.a; b+=ev.b; x+=ev.x}
        a/=evs.size; b/=evs.size; x/=evs.size
        (new EdgeVal(a,b,x), evs.map(_._1))
      }

      // [(eid,[(i,ev)])]  ==> [eid, (ev, [i])]
      val updatedEdges : spark.RDD[(EdgeId,(EdgeVal,Seq[Int]))] =
        edgeGroups.mapValues(consolidateEdgeUpdates)

      // Create ordered list of edge update sets for each vertex
      // [(eid, (ev,[i])) ] ==> [(i,[eu])]
      val vertexUpdatedEdges : spark.RDD[(Int,Seq[EdgeUpdate])] = 
        updatedEdges                                          // [(eid, (ev,[i]))]
          .flatMap {case (eid,(ev,is)) =>                           
                      is.map( (_,new EdgeUpdate(eid,ev))) }   // [(i, eu)]
          .groupByKey                                         // [(i, [eu])]

      def applyEdgeUpdatesToVertex(vc:VertexCache, eus:Seq[EdgeUpdate]) = {
        eus.foreach( eu => {
          val i = vc.edgeId2EdgePos(eu.id)
          val e : Edge = vc.v.edges(i)
          e.a = eu.value.a
          e.b = eu.value.b
          e.x = eu.value.x
        })
        vc
      }

      vertices = 
        vertices.join(vertexUpdatedEdges)                             // [(i,(vc,[eu]))]
                 .mapValues {case (vc:VertexCache,eus:Seq[EdgeUpdate]) =>
                        applyEdgeUpdatesToVertex(vc,eus)}

    }

    val finalEdges : Array[Edge] =
      vertices                                                // [(i,vc)]
        .values                                               // [vc]
        .flatMap (_.v.edges.map(e=>(new EdgeId(e),e)))        // [(eid,e)]
        .reduceByKey((e1,e2)=>e1)                             // [(eid,e)]
        .values.toArray                                       // [e]

    println(finalEdges.mkString("\n"))

  }

}

