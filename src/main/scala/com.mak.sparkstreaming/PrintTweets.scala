package com.mak.sparkstreaming;

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import Utilities._

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()

    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val SPARK_HOME = "/Users/mntiwari/Installables/spark-1.6.2-bin-hadoop2.6"

    // 1 Second is the batch interval;
    val ssc = new StreamingContext("local[*]", "PrintTweets", Seconds(1))

    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())

    //length of tweets
    val lengths = statuses.map(status => status.length)

    // Print out the first ten
    //statuses.print()

    //Print tweets to a file
    /*
    var totalTweets: Long = 0

    statuses.foreachRDD( (rdd, time) => {

      if (rdd.count >0) {

        //Combine all partitions of rdd in to 1 rdd (basically take each rdd from every cluster and consolidate in to one rdd)
        val repartitionRDD = rdd.repartition(1).cache()

        repartitionRDD.saveAsTextFile("Tweets_" + time.milliseconds.toString)

        totalTweets += repartitionRDD.count()
        println("Tweet count: "+ totalTweets)
        if (totalTweets>1000) {
          System.exit(0)
        }
      }

    })
    */

    val totalTweets = new AtomicLong(0)
    val totalChars = new AtomicLong(0)
    val longestTweet = new AtomicLong(0)

    lengths.foreachRDD( (rdd, time) => {

      if (rdd.count>0) {
        totalTweets.getAndAdd(rdd.count)

        //sum all tweet lengths of the rdd to get the total tweetlength of that rdd
        totalChars.getAndAdd(rdd.reduce(_ + _))

        longestTweet.set(rdd.max())

        println("Total tweets: " + totalTweets +
                "\ttotal characters: "+ totalChars +
                "\tAverage: " + totalChars.get / totalTweets.get +
                "\t longest tweet length: "+ longestTweet)
      }
    })

    //Checkpoint directory
    ssc.checkpoint("/Users/mntiwari/Udemy/Spark/checkpointDir/")

    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }
}