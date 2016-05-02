package almeida.rochapaulo.spark.apps

import java.io.{BufferedOutputStream, PrintWriter}
import java.net.{ServerSocket, Socket}
import java.util.concurrent.{Executors, TimeUnit}

import almeida.rochapaulo.spark.utils.Logging
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
  * AttackDetector
  *
  * It is an fake application which shows a
  * near real-time processing using Apache Spark Streaming
  * to identify DDoS attacks in a server.
  *
  * @author rochapaulo on 28/04/16.
  *
  */
object AttackDetector extends App {

  def PORT = 9999
  def HOST = "localhost"

  /**
    * AttackDetector application entry point
    *
    * @param args
    */
  override def main(args : Array[String]): Unit = {

    val executor = Executors.newFixedThreadPool(2)
    try {
      executor.execute(new Server(PORT))
      executor.execute(new AttackDetector(HOST, PORT))
    } finally {
      executor.shutdown()
    }

  }

  /**
    * AttackDetector Spark Job
    *
    * This job connects to a given host and listen to that port for incoming
    * data, those data is processed by spark at NRT looking for potential
    * attacks (multiple accesses from same origin in a very short time period)
    *
    * @param host : host to listen
    * @param port : port to listen
    *
    */
  private class AttackDetector(host : String, port : Int) extends Runnable with Serializable with Logging {

    override def run() : Unit = {

      val sparkConf =
        new SparkConf()
          .setMaster("local[*]")
          .setAppName(AttackDetector.getClass.getSimpleName)

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

    /**
      * Maps request line log to a key pair value (IP address, Timestamp)
      *
      * @return pair of (IP : String, Timestamp : Long)
      */
    def ip_Timestamp : (String) => (String, Long) = {
      line => {
        val values = line.split(" ")
        (values(3), values(5).toLong)
      }
    }

    /**
      * Filters requests made in a time interval
      * less than 5 seconds
      *
      * @return Boolean
      */
    def recent : ((String, Long)) => Boolean = {
      tuple => {
        val accessTime = TimeUnit.MILLISECONDS.toSeconds(tuple._2)
        val currentTime = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis())
        currentTime - accessTime <= 5
      }
    }

    /**
      * Scores each IP with value 1
      *
      * @return pair of (IP : String, 1)
      */
    def scoreRequests: ((String, Long)) => (String, Int) = {
      tuple => (tuple._1, 1)
    }

    /**
      * Sum arguments
      *
      * @return Int
      */
    def count : (Int, Int) => Int = _ + _

    /**
      * Filters possible attack, but whats characterize an attack?
      * This function consider if same IP sent more than 5 requests
      *
      * @return Boolean
      */
    def potentialAttack : ((String, Int)) => Boolean = _._2 > 5

    /**
      * Log attack info to file
      *
      * @return Unit
      */
    def log2File: (RDD[(String, Int)]) => Unit = {
      _.foreach(access => {
        logger.warn(s"IP[${access._1}] tried access ${access._2} times in very short time interval")
      })
    }

  }

  /**
    * Mock SocketServer
    *
    * this socketServer generate and writes fake incoming requests
    * to localhost in a given port
    *
    * @param port : SocketServer port
    *
    */
  private class Server(port : Int) extends Runnable with Logging {

    val executor = Executors.newFixedThreadPool(10)
    val logServer = new ServerSocket(port)

    override def run(): Unit = {

      logger.info(s"SocketServer at localhost:${port}")

      try {
        while (true) {
          val logSocket = logServer.accept()
          executor.execute(new Handler(logSocket))
        }
      } finally {
        executor.shutdown()
      }

      logger.info("SocketServer finished")
    }

    private class Handler(socket : Socket) extends Runnable {

      val addresses = Seq("82.225.34.68", "34.224.10.78", "76.118.13.181", "120.65.65.158", "156.24.208.55")

      override def run(): Unit = {

        val writer = new PrintWriter(new BufferedOutputStream(socket.getOutputStream), true)
        for (i <- 1 to 15) {
          logger.info(s"Request received from ${getAddress()} at ${System.currentTimeMillis()}")
          writer.println(s"Request received from ${getAddress()} at ${System.currentTimeMillis()}")
        }
        writer.flush()
        writer.close()

      }

      def getAddress() : String = addresses(new Random().nextInt(addresses.length - 1))

    }

  }

}
