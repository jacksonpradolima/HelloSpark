package almeida.rochapaulo.spark.streaming.apps.AttackDetector

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by rochapaulo on 28/04/16.
  */
object AttackDetector extends App with Logging {

  override def main(args : Array[String]): Unit = {

    logger.info(s"Starting AttackDetector at ${

      val sdf = new SimpleDateFormat("mm/dd/yyyy : hh:mm:ss")
      sdf.format(Calendar.getInstance().getTime)

    }")

    (new AttackDetector(Server.HOST, Server.PORT)).run()

    logger.info("AttackDetector finished")
  }


  private class AttackDetector(host : String, port : Int) extends Runnable {

    override def run() : Unit = {

      val sparkConf =
        new SparkConf()
          .setMaster("local[*]")
          .setAppName(Server.getClass.getSimpleName)

      val ssc = new StreamingContext(sparkConf, Seconds(2))
      val stream = ssc.socketTextStream(host, port)

      logger.info(s"AttackDetector connected to ${host}:${port}")

      stream.filter(line => line.startsWith("Request"))
        .map(ip_Timestamp)
        .filter(recent)
        .map(scoreRequests)
        .reduceByKey(count)
        .filter(potentialAttack)
        .foreachRDD(log2File)

      ssc.start()
      ssc.awaitTermination()

    }

    def ip_Timestamp : (String) => (String, Long) = {
      line => {
        val values = line.split(" ")
        (values(3), values(5).toLong)
      }
    }

    def recent : ((String, Long)) => Boolean = {
      tuple => {
        val accessTime = TimeUnit.MILLISECONDS.toSeconds(tuple._2)
        val currentTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())
        currentTime - accessTime <= 5
      }
    }

    def scoreRequests: ((String, Long)) => (String, Int) = {
      tuple => (tuple._1, 1)
    }

    def count : (Int, Int) => Int = _ + _

    def potentialAttack : ((String, Int)) => Boolean = _._2 > 5

    def log2File: (RDD[(String, Int)]) => Unit = {
      _.foreach(access => {
        logger.warn(s"IP[${access._1}] tried access ${access._2} times in very short time interval")
      })
    }

  }

}
