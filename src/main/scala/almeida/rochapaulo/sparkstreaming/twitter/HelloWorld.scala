package almeida.rochapaulo.sparkstreaming.twitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by rochapaulo on 27/04/16.
  */
object HelloWorld extends App {

  System.setProperty("twitter4j.oauth.consumerKey", "RwRVDyEcQCnTntxBxC6cEF1Nv");
  System.setProperty("twitter4j.oauth.consumerSecret", "jLPoHukIPSwwzx8rbjla10qq7ndAOD7STPTzwDRxPs5UaTkR3w");
  System.setProperty("twitter4j.oauth.accessToken", "2774751856-D4zuU8tecKsQWlqWD7IhhV8sZwuuhdWkvJDTrdi");
  System.setProperty("twitter4j.oauth.accessTokenSecret", "ZGgF1Uv6iTaCgLM7Rr6mYA8zcq6GYcN4ktcxuxYSmtGsY");

  System.setProperty("almeida.rochapaulo.twitter-streaming.spark.app-name", "HelloWorld");
  System.setProperty("almeida.rochapaulo.twitter-streaming.spark.master", "local[2]");
  System.setProperty("almeida.rochapaulo.twitter-streaming.spark.window-seconds", "5");

  val sparkConf = new SparkConf()
    .setAppName("HelloWorld")
    .setMaster("local[2]");

  val windowSize = Seconds(10)

  val filters = Seq()
  val ssc = new StreamingContext(sparkConf, windowSize)
  val stream = TwitterUtils.createStream(ssc, None, filters)


  stream
    .flatMap(_.getHashtagEntities)
    .map(_.getText)
    .print()

  ssc.start()
  ssc.awaitTermination()

}
