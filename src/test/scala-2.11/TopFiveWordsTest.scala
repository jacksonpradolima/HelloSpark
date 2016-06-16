import almeida.rochapaulo.spark.webinar.TopFiveWords
import almeida.rochapaulo.spark.webinar.Word
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
  * Created by rochapaulo on 12/06/16.
  */
class TopFiveWordsTest extends FunSuite with BeforeAndAfter {

  private var sc : SparkContext = _

  before {
    val sparkConf =
      new SparkConf()
        .setAppName("TopFiveWords")
        .setMaster("local[*]")

    sc = new SparkContext(sparkConf)
  }

  after {
    sc.stop()
  }

  test("count top five quoted words stored in a text file") {

    val textFileRDD = sc.textFile(getClass.getResource("what_is_spark").getPath)

    val topFive = TopFiveWords.run(sc, textFileRDD)

    println(topFive)
    assert(topFive(0) === new Word("Spark", 13))
    assert(topFive(1) === new Word("distributed", 9))
    assert(topFive(2) === new Word("data", 8))
    assert(topFive(3) === new Word("Apache", 7))
    assert(topFive(4) === new Word("that", 7))

  }


  test("count top five quoted words store inside an array") {

    val values = Array(
        "Webinar", "MATERA Systems", "Maringá", "Scala", "Teste", "Webinar", "MATERA",
        "Scala", "Webinar", "Campinas", "Programação", "BigData", "Webinar", "Teste",
        "Online", "MATERA Systems", "MATERA", "Big", "Spark", "MATERA Systems",
        "Systems Citadas BigData", "MATERA", "Systems", "Spark", "BigData", "Contador",
        "ApacheSpark", "ApacheSpark", "ApacheSpark", "ApacheSpark", "ApacheSpark", "ApacheSpark",
        "ApacheSpark")

    val arrayRDD = sc.parallelize(values)

    val topFive = TopFiveWords.run(sc, arrayRDD)

    assert(topFive(0) === new Word("ApacheSpark", 7))
    assert(topFive(1) === new Word("MATERA", 6))
    assert(topFive(2) === new Word("Systems", 5))
    assert(topFive(3) === new Word("Webinar", 4))
    assert(topFive(4) === new Word("BigData", 3))

  }

}
