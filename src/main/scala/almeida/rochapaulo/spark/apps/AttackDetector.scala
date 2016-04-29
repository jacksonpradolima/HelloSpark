package almeida.rochapaulo.spark.apps

import java.util.concurrent.TimeUnit

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
  * Created by rochapaulo on 28/04/16.
  */
object AttackDetector extends App with Logging {

  override def main(args : Array[String]): Unit = {

    val uri = Thread.currentThread().getContextClassLoader.getResource("log4j.properties")
    PropertyConfigurator.configure(uri)

    logger.info("Starting Program")
    (new AttackDetector(9999)).run()
  }


  private class AttackDetector(port : Int) extends Runnable {

    override def run() : Unit = {

      val sparkConf =
        new SparkConf()
          .setMaster("local[2]")
          .setAppName(Server.getClass.getSimpleName)

      val ssc = new StreamingContext(sparkConf, Seconds(2))
      val stream = ssc.socketTextStream("localhost", port)

      stream.filter(line => line.startsWith("Request"))
        .map(line => {
          val values = line.split(" ")
          (values(3), values(5))
        })
        .filter(tuple => {
          val accessTime = TimeUnit.MILLISECONDS.toSeconds(tuple._2.toLong)
          val currentType = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())

          currentType - accessTime < 5
        })
        .map(tuple => (tuple._1, 1))
        .reduceByKey(_ + _)
        .filter(_._2 > 5)
        .foreachRDD(_.foreach(access => logger.warn(s"IP[${access._1}] tried access ${access._2} times in 5 seconds")))

      ssc.start()
      ssc.awaitTermination()

    }

  }

}
