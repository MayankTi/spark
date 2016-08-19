package com.mak.clickstream

import com.mak.sparkstreaming.Utilities
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.regex.Matcher

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

import scala.util.Try

/**
  * Created by mntiwari on 7/17/16.
  */
object LogSQL {

  case class Record(url: String, status: Int, agent:String)

  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[*]", "LogSQL", Seconds(1))

    Utilities.setupLogging()

    val pattern = Utilities.apacheLogPattern()

    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    //Get URL, status, user agent
    val requests = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = Try(requestFields(1)).getOrElse("[error]")
        (url, matcher.group(6).toInt, matcher.group(9))
      } else {
        ("error", 0, "error")
      }
    })

    requests.foreachRDD( (rdd, time) => {

      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
      import sqlContext.implicits._

      val requestsDF = rdd.map(w => Record(w._1, w._2, w._3)).toDF()

      requestsDF.registerTempTable("requests")

      val wordCountDF = sqlContext.sql("select agent, count(*) as total from requests group by agent")
      println(s"----- $time --------")
      wordCountDF.show()

      requestsDF.write.json("jsonDump_" + time.milliseconds)
    })

    ssc.checkpoint("/Users/mntiwari/Udemy/Spark/checkpointDir/")
    ssc.start()
    ssc.awaitTermination()
  }

  object SQLContextSingleton {

    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }
}
