package com.mak.clickstream

import java.util.regex.Matcher

import com.mak.sparkstreaming.Utilities
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._

import scala.util.Try

/**
  * Created by mntiwari on 7/18/16.
  */

object Sessionizer {

  case class SessionData(val sessionLength: Long, var clickstream: List[String])

  def trackStateFunc( batchTime: Time, ip: String, url: Option[String], state: State[SessionData] ): Option[(String, SessionData)] = {

    val previousState = state.getOption().getOrElse(SessionData(0, List()))

    val newState = SessionData(previousState.sessionLength+1L, previousState.clickstream :+ url.getOrElse("empty"))

    state.update(newState)

    Some((ip, newState))
  }

  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[*]", "Sessionizer", Seconds(1))

    Utilities.setupLogging()

    val pattern = Utilities.apacheLogPattern()

    val stateSpec = StateSpec.function(trackStateFunc _).timeout(Minutes(30))

    //nc -4 127.0.0.1 -kl 9999 < access_log.txt
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val requests = lines.map(x => {

      val matcher: Matcher = pattern.matcher(x)
      if (matcher.matches()) {
        val ip = matcher.group(1)
        val request = matcher.group(5)
        val requestFields = request.toString.split(" ")
        val url = Try(requestFields(1)).getOrElse("[error]")
        (ip, url)
      } else {
        ("error","error")
      }

    })

    val requestWithState = requests.mapWithState(stateSpec)

    val snapshotStream = requestWithState.stateSnapshots()

    snapshotStream.foreachRDD( (rdd, time) => {

      val sqlContext = new SQLContext(rdd.sparkContext)
      import sqlContext.implicits._

      val requestDF = rdd.map(x => (x._1, x._2.sessionLength, x._2.clickstream)).toDF("ip", "sessionLength", "clickstream")

      requestDF.registerTempTable("sessionData")

      val sessionsDF = sqlContext.sql("select * from sessionData")
      println(s"-------$time-------")
      sessionsDF.show()

    })

    ssc.checkpoint("/Users/mntiwari/Udemy/Spark/checkpointDir/")
    ssc.start()
    ssc.awaitTermination()

  }

}
