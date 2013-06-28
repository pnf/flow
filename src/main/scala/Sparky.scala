import spark.SparkContext
import SparkContext._
import scala.collection.mutable.HashMap  
object SimpleJob {
  def main(args: Array[String]) {
    val logFile = "/var/log/system.log" // Should be some file on your system
    val jars = List("/Users/pnf/dev/flow/target/scala-2.9.2/flow_2.9.2-1.0.jar")
    val env = new HashMap[String,String]()
    val sc = new SparkContext("local", "Simple Job", "/Users/pnf/dist/spark-0.7.2",jars,env)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
