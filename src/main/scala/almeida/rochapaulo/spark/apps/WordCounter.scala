package almeida.rochapaulo.spark.apps

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by rochapaulo on 01/05/16.
  */
object WordCounter extends App {


  def highestScoring : Ordering[(String, Int)] = new Ordering[(String, Int)] {
    override def compare(x: (String, Int), y: (String, Int)): Int = if (x._2 > y._2) -1 else 1
  }


  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName(WordCounter.getClass.getSimpleName)

  val context = new SparkContext(sparkConf)

  val topFive =
    context.textFile("src/main/resources/whatIsSpark.text")
      .flatMap(_.split(" "))
      .filter(_.length >= 5)
      .map(_ -> 1)
      .reduceByKey(_ + _)
      .takeOrdered(5)(highestScoring)

  for ((word, count) <- topFive) {
    println(s"$word -> ${Console.GREEN} $count ${Console.RESET}")
  }

  context.stop()

}
