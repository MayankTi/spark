package com.mak.scalaspark

import java.nio.charset.CodingErrorAction

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.{Codec, Source}

/**
  * Created by mntiwari on 8/7/16.
  */
object MovieSimilarities {

  case class movieItem(id:Int, title:String, releaseDate:String)

  def loadMovieNames(): Map[Int, movieItem] = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    val items = Source.fromFile("/Users/mntiwari/Udemy/datasets/ml-100k/u.item").getLines()

    var movieNames: Map[Int, movieItem] = Map()

    for(item <- items) {

      val fields = item.split('|')
      if (fields.size>3) {
        val id = fields(0).toInt
        movieNames += (id -> new movieItem(id, fields(1), fields(2)))
      } else {
        println(s"Bad request $fields")
      }
    }
    movieNames
  }

  type MovieRating = (Int, Double)
  type UserRatingPair = (Int, (MovieRating, MovieRating))

  def makePairs(userRatings:UserRatingPair) = {

    val movieRating1 = userRatings._2._1
    val movieRating2 = userRatings._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1
    val rating1 = movieRating1._2
    val rating2 = movieRating2._2

    ((movie1, movie2) , (rating1, rating2))
  }

  def filterDuplicates(userRatingPair: UserRatingPair): Boolean = {

    val movieRating1 = userRatingPair._2._1
    val movieRating2 = userRatingPair._2._2

    val movie1 = movieRating1._1
    val movie2 = movieRating2._1

    movie1 < movie2
  }

  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]

  def computeCosineSimilarity(ratingPairs: RatingPairs) : (Double, Int) = {

    var xx = 0.0
    var yy = 0.0
    var xy = 0.0
    var occurrence = 0

    for (rating <- ratingPairs) {
      xx += rating._1 * rating._1
      yy += rating._2 * rating._2
      xy += rating._1 * rating._2
      occurrence += 1
    }

    val similarity = xy / (math.sqrt(xx) * math.sqrt(yy))

    (similarity, occurrence)
  }

  def main(args: Array[String]) {

    val config = new SparkConf()
    config.setAppName("MovieSimilarities").setMaster("local[*]")

    val sc = new SparkContext(config)

    val movieNames = sc.broadcast(loadMovieNames())

    println(movieNames.value.size)

    val data = sc.textFile("/Users/mntiwari/Udemy/datasets/ml-100k/u.data")

    val ratings = data.map( line => line.split("\t")).map(l => (l(0).toInt, (l(1).toInt, l(2).toDouble )))

    //self joining rdd to get all movie combinations
    val joinedRatings = ratings.join(ratings)

    val uniqueRatings = joinedRatings.filter(filterDuplicates)

    val moviePairs = uniqueRatings.map(makePairs)

    val moviePairRatings = moviePairs.groupByKey()

    val moviePairSimilarity = moviePairRatings.mapValues(computeCosineSimilarity).cache()

    val sorted = moviePairSimilarity.sortByKey()
    sorted.saveAsTextFile("/Users/mntiwari/Udemy/output/")


  }
}
