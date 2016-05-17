package almeida.rochapaulo.spark.apps

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by rochapaulo on 16/05/16.
  */
case class WordCount(word : String, count : Int)
object WordCounter {

  def count(sc : SparkContext, lines : RDD[String]) : RDD[WordCount] = {

    val result =
      lines.flatMap(_.split(" "))
        .map((_, 1))
        .reduceByKey(_ + _)
        .map({
          case (word : String, count : Int) => WordCount(word, count)
        })

    result.sortBy(_.count)
  }

}
