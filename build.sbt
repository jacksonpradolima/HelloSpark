name := "spark-streaming"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "1.6.1"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % sparkVersion
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % sparkVersion
libraryDependencies += "log4j" % "log4j" % "1.2.17"