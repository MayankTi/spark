package com.mak.sparkstreaming;

import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import Utilities._

object PopularHashtags {

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

    //Extract text from the tweets
    val statuses = tweets.map(tweet => tweet.getText)
    val tweetWords = statuses.flatMap(line => line.split(" "))
    val hashtags = tweetWords.filter(word => word.startsWith("#"))

    //create key value rdds for each hashtag
    val hashtagKeyValue = hashtags.map( tag => (tag, 1))
    val wordsKeyValue = tweetWords.map( (_ ,1))

    val hashtagCount = hashtagKeyValue.reduceByKeyAndWindow( (x,y) => x+y, (x,y) => x-y,
                                          /* Window size */   /* Slide size */
                                          Seconds(300),       Seconds(1))

    val trendingWords = wordsKeyValue.reduceByKeyAndWindow(_+_, _-_, Seconds(3600), Seconds(5))

    //reduce by window returns only a single RDD always

    //sort the output by value in descending order
    val sortedCount = hashtagCount.transform( rdd => rdd.sortBy(x => x._2 ,false))

    val sortedWords = trendingWords.transform( rdd => rdd.sortBy(x => x._2, false))

    println("Hashtag trends: ")
    sortedCount.print()

    println("Word trends: ")
    sortedWords.print()

    ssc.checkpoint("/Users/mntiwari/Udemy/Spark/checkpointDir/")

    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }
}