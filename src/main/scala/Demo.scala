package com.podsnap.flow
//import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.mutable.HashSet
//import scala.math._
import breeze.linalg._
import breeze.numerics._
import scala.collection.mutable.HashMap
import spark.SparkContext
import SparkContext._


object Demo  extends App  {

  val jars = List("/Users/pnf/dev/flow/target/scala-2.9.2/flow_2.9.2-1.0.jar")
  val env = new HashMap[String,String]()
  val sc = new SparkContext("local", "Simple Job", "/Users/pnf/dist/spark-0.7.2",jars,env)
      
  // create big random graph.
  // every edge should be connected to someone
  val beta_cost = 0.2
  val beta_flow = 0.2
  val default_gain = 0.1
  val imbalance_gain = 100.0
  val source_gain = 100.0
  val n_iter = 20

/*
  val edges = new HashSet[Edge]()
  edges.add(new Edge(-1,0,target=10.0,gain=source_gain))
  edges.add(new Edge(-1,1,target=(-3.0),gain=source_gain))
  edges.add(new Edge(-1,2,target=(-7.0),gain=source_gain))
  edges.add(new Edge(0,1,target=0.0,gain=default_gain))
  edges.add(new Edge(0,2,target=0.0,gain=default_gain))
  val g = new Graph(edges)
*/

  val g = Graph.random(10,2,3,
		       10.0,5.0,
		       default_gain, source_gain)

  val solver = new Solver(g, n_iter,
			  beta_cost, beta_flow,
			  imbalance_gain)
  solver.solve(sc)

}
