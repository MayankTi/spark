package com.mak.sparkstreaming

import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by mntiwari on 7/21/16.
  */
object StreamingKMeans {

  def main(args: Array[String]) {

    val ssc = new StreamingContext("local[*]", "StreamingKMeans", Seconds(1))

    Utilities.setupLogging()

    val trainingLines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    val testingLines = ssc.socketTextStream("127.0.0.1", 7777, StorageLevel.MEMORY_AND_DISK_SER)

    val trainingData = trainingLines.map(Vectors.parse).cache()

    val testingData = testingLines.map(LabeledPoint.parse)

    trainingData.print()

    val model = new StreamingKMeans().setDecayFactor(1.0).setK(5).setRandomCenters(2, 0.0)

    model.trainOn(trainingData)

    model.predictOnValues(testingData.map(lp => (lp.label toInt, lp.features))).print()

    ssc.checkpoint("/Users/mntiwari/Udemy/Spark/checkpointDir/")
    ssc.start()
    ssc.awaitTermination()

  }

}
