package almeida.rochapaulo.spark.webinar

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by rochapaulo on 12/06/16.
  */
object TopFiveWords {

  private val WHITE_SPACE = " "

  def run(sc: SparkContext, line : RDD[String]): Seq[Word] = {

    line.flatMap(_.split(WHITE_SPACE))
        .filter(word => word.length > 3)
        .map(word => (word, 1))
        .reduceByKey(_ + _)
        .map({
          case (value : String, times : Int) => new Word(value, times)
        })
        .takeOrdered(5)(moreQuoted)

  }

  private val moreQuoted : Ordering[Word] = new Ordering[Word] {
    override def compare(x: Word, y: Word): Int =
      y.times.compare(x.times)
  }

}
