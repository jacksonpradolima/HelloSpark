package almeida.rochapaulo.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by prra on 21/07/16.
  */
object SparkSQLHelloExample extends App {

  val masterURL = "local[*]"
  val sparkConf = new SparkConf().setAppName("HelloWorld App").setMaster(masterURL)
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.read.json("/home/prra/workspace/HelloSpark/src/test/resources/people.json")

  df.registerTempTable("people")
  sqlContext.sql("SELECT name FROM people WHERE age > 15").show()

}
