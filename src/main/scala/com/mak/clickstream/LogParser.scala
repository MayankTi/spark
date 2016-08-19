package com.mak.clickstream

import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.mak.sparkstreaming.Utilities
import org.apache.spark.storage.StorageLevel
import java.util.regex.Matcher

/**
  * Created by mntiwari on 7/17/16.
  */
object LogParser {

  def main(args: Array[String] ) {

    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))

    Utilities.setupLogging()

    val pattern = Utilities.apacheLogPattern()

    // nc -4 127.0.0.1 -kl 9999 < access_log.txt
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val requests = lines.map(x => { val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5) })

    val urls = requests.map(x => {val arr = x.toString.split(" "); if (arr.size == 3) arr else s"[error]: $arr" })

    val urlCount = urls.map(x => (x,1)).reduceByKeyAndWindow(_+_, _-_, Seconds(300), Seconds(1))

    val sortedUrls = urlCount.transform( rdd => rdd.sortBy(_._2, false))

    sortedUrls.print()

    ssc.checkpoint("/Users/mntiwari/Udemy/Spark/checkpointDir/")
    ssc.start()
    ssc.awaitTermination()
  }

}
