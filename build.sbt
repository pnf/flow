import AssemblyKeys._ // put this at the top of the file

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "jasper", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
    case "application.conf" => MergeStrategy.concat
    case "unwanted.txt"     => MergeStrategy.discard
    case "about.html" => MergeStrategy.first
    case x => old(x)
  }
}

name := "flow"

version := "1.0"

//scalaVersion := "2.10.2"

scalaVersion := "2.9.2"

libraryDependencies += "org.clapper" %% "grizzled-slf4j" % "0.6.10"  // 2.9

// libraryDependencies += "com.typesafe" %% "scalalogging-slf4j" % "1.0.1"  //2.10

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.5"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "1.9.1" % "test"

//libraryDependencies += "org.scalanlp" % "breeze_2.10" % "0.3"

// libraryDependencies += "org.scalanlp" % "breeze_2.9.2" % "0.2"

libraryDependencies += "org.scalanlp" % "breeze-core_2.9.2" % "0.2"

libraryDependencies += "org.scalanlp" % "breeze-math_2.9.2" % "0.2"

libraryDependencies += "org.spark-project" % "spark-core_2.9.3" % "0.7.2"

libraryDependencies += "com.github.tototoshi" % "scala-csv_2.9.2" % "0.8.0"

// libraryDependencies += "net.sf.jgrapht" % "jgrapht" % "0.8.3"
