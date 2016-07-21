import org.apache.spark.{SparkConf, SparkContext}
import almeida.rochapaulo.spark.core.WordCounter
import almeida.rochapaulo.spark.core.{WordCount, WordCounter}
import org.scalatest.{FunSuite, Matchers}


/**
  * Created by rochapaulo on 16/05/16.
  */
class WordCounterTest extends FunSuite with Matchers {

  test("CountWords") {

    val conf =
      new SparkConf()
        .setAppName("WordCounter")
        .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val lines = Seq("well",  "well",  "well", "there there", "lol lol lol")

    val linesRDD = sc.parallelize(lines)

    val result = WordCounter.count(sc, linesRDD).collect()

    result should contain
      allOf(
        WordCount("well", 3),
        WordCount("there", 2),
        WordCount("lol", 3)
      )

    sc.stop()

  }

}
