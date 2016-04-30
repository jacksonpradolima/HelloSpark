package almeida.rochapaulo.spark.streaming.apps.AttackDetector

import java.io._
import java.net.{ServerSocket, Socket}
import java.util.concurrent.{Executors}

import scala.util.Random

/**
  * Created by rochapaulo on 28/04/16.
  */
object Server extends App with Logging {

  def PORT = 9999
  def HOST = "localhost"

  override def main(args : Array[String]) : Unit = {

    val executor = Executors.newSingleThreadExecutor()
    try {
      executor.execute(new Server(PORT))
    } finally {
      executor.shutdown()
    }

  }

  private class Server(port : Int) extends Runnable {

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

    class Handler(socket : Socket) extends Runnable {

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
