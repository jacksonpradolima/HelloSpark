package almeida.rochapaulo.spark.apps

import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld extends App {

  val masterURL = "local[*]"

  val sparkConf = new SparkConf()
    .setAppName("HelloWorld App")
    .setMaster(masterURL)

  val sc = new SparkContext(sparkConf)

  val lines = sc.textFile("src/main/resources/whatIsSpark.text")


  val c = lines.count()
  println(s"There are $c lines on file: whatIsSpark.text")


  sc.parallelize(1 to 10000)

}

