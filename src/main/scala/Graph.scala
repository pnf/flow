package com.podsnap.flow
import scala.collection.mutable.HashSet
import breeze.linalg._
import breeze.numerics._
import scala.util.Random
//import com.typesafe.scalalogging.slf4j.Logging
import grizzled.slf4j.Logging

@serializable class Graph(val edges: HashSet[Edge]) extends Logging {

  val vertices : Array[Vertex]= {
    val n_vertices = edges.map(e=>math.max(e.i,e.j)).reduce(math.max(_,_)) + 1
    val vtmp  = (1 to n_vertices).map(_ => List[Edge]()).toArray
    for(e <- edges) {
      if(e.i>=0)
	vtmp(e.i) = e :: vtmp(e.i)
      if(e.j>=0)
	vtmp(e.j) = e :: vtmp(e.j)
    }
    vtmp.zipWithIndex.map {ei => new Vertex(ei._2,ei._1)}
  }

  val edgeId2VertexIndices : Map[EdgeId,HashSet[Int]] = {
    val ret = edges.map(e => (e.id, new HashSet[Int]()) ).toMap
    for( (v,i)<-vertices.zipWithIndex; e<-v.edges) {
      ret(e.id) += i
    }
    ret
  }

}

@serializable class Edge(ii:Int, jj:Int, val target:Double=0.0, val gain:Double=0.1) {
  val i = if(ii<jj) ii else jj
  val j = if(ii>jj) ii else jj

  val id = new EdgeId(i,j)
  
  var x = 0.0    // current flow
  var a = gain   // gain, including neighbors
  var b = target // target, including neighbors


  // only store one edge between vertices
  override def equals(a:Any) = a match {case e : Edge => i==e.i && j==e.j
					case _ => false}
  override def hashCode = i.hashCode ^ j.hashCode

  def nontrivial = i!=j
  //override def toString = f"Edge($i%02d, $j%02d, a=$a%6.2f b=$b%6.2f x=$x%6.2f, target=$target%6.2f, gain=$gain%6.2f)"

  override def toString = "%02d, %02d, a=%6.2f b=%6.2f x=%6.2f, target=%6.2f, gain=%6.2f)".format(i,j,a,b,x,target,gain)
}

@serializable class EdgeId(val i: Int, val j: Int) {
  def this(e:Edge) = this(e.i,e.j)
  override def equals(a:Any) = a match {
    case e : EdgeId => i==e.i && j==e.j
    case _ => false}
  override def hashCode = i.hashCode ^ j.hashCode
  override def toString = "i=" +i+ ", j="+j
}
@serializable class EdgeVal(val a: Double, val b: Double, val x:Double) {
  def this(e:Edge) = this(e.a,e.b,e.x)
  def add(e:Edge) = new EdgeVal(a+e.a,b+e.b,x+e.x)
  override def toString = "a="+a+",b="+b+",x="+x
}

@serializable class EdgeUpdate(val id:EdgeId, val value:EdgeVal) {
  override def toString = "%02d, %02d, a=%6.2f b=%6.2f x=%6.2f".format(id.i,id.j,value.a,value.b,value.x)
}

@serializable class Vertex(val i:Int, ee:Seq[Edge]) {
  val edges : Array[Edge] = ee.toArray
  val direction = edges.map {e => if(e.j==i) 1.0 else -1.0}  // incoming +1, outgoing -1

  /*
  assert(vertices.forall(_.edges.size>=min_degree))
  logger.debug(s"${val stat = vertices.map(_.edges.size).foldLeft((0,0.0,0.0))((p,x)=>(p._1+1,p._2+x,p._3+x*x))
	       val mean = stat._2/stat._1
	       val sd = (sqrt(stat._3/stat._1 - mean*mean))
	       (mean,sd)}")
  assert(abs(edges.map(_.target).reduce(_+_))<1.e-8)
  */

  private val cr = "\n"
  def flow = edges.zip(direction).map(_ match {case (e,d)=>e.x*d} ).sum

  override def toString = "i=" + i + " n=" +  edges.size + " flow=" +flow;
}
  
object Graph {

  def random(n_vertices:Int, min_degree:Int, avg_degree:Int,
	   total_source:Double, max_indiv_source:Double,
	   default_gain:Double, source_gain:Double) : Graph = {

    val edges = new HashSet[Edge]()

    // assure that all vertices are of minimum degree out and in
    def assureMinDegree() {
      val degrees = Array.ofDim[Int](n_vertices)
      for(i<-0 until n_vertices) {
	while(degrees(i)<min_degree) {
	  val e = new Edge(i,Random.nextInt(n_vertices),gain=default_gain)
	  if(!edges.contains(e) && e.nontrivial) {
	    edges.add(e)
	    degrees(e.i) = degrees(e.i)+1
	    degrees(e.j) = degrees(e.j)+1
	  }
	}
      }
    }
    assureMinDegree()

    // and that we've achieved average degree
    while (edges.size < avg_degree*n_vertices) {
      val e = new Edge(Random.nextInt(n_vertices),Random.nextInt(n_vertices),gain=default_gain)
      if(!edges.contains(e) && e.nontrivial) {
	edges.add(e)
      }
    }

    def addSources(sign:Double) {
      var sofar = 0.0
      while(sofar<total_source) {
	var x = scala.math.min(Random.nextDouble()*max_indiv_source,total_source-sofar)
	val e = new Edge(-1,Random.nextInt(n_vertices),target=x*sign,gain=source_gain)
	if(e.nontrivial && !edges.contains(e)) {
	  sofar = sofar+x
	  edges.add(e)
	}
      }
    }
    addSources(1.0); addSources(-1.0)
    
    return new Graph(edges)
  }
}
