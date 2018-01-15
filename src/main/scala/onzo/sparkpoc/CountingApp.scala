package onzo.sparkpoc

import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import onzo.sparkpoc.CountingApp.args

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object CountingLocalApp extends App{
  val inputFile = args(0)


  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("my awesome app")
    .set("spark.cassandra.connection.host", "localhost")

  // spark-submit command should supply all necessary config elements
  Runner.run(conf, inputFile, s"/tmp/spark_out_${System.currentTimeMillis()}")

}

/**
  * Use this when submitting the app to a cluster with spark-submit
  * */
object CountingApp extends App{
  val inputFile = args(0)

  // spark-submit command should supply all necessary config elements
  Runner.run(new SparkConf(), inputFile, s"/tmp/spark_out_${System.currentTimeMillis()}")
}

object Runner {
  def run(conf: SparkConf, inputFile: String, outputFile: String): Unit = {
    val sc = new SparkContext(conf)
    val rdd = sc.textFile(inputFile)


    //println(rdd.map(line => sc.cassandraTable("onzo", "household_api_users").count()).distinct().count())
    val counts = WordCount.withStopWordsFiltered(rdd)

    
    counts.saveToCassandra("onzo", "test")

    //println(sc.cassandraTable("onzo", "household_api_users").count())
  }
}
