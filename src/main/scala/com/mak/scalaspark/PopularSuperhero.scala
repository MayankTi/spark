package com.mak.scalaspark

import java.nio.charset.CodingErrorAction

import org.apache.spark.{SparkConf, SparkContext}

import scala.io.{Codec, Source}

/**
  * Created by mntiwari on 8/6/16.
  */
object PopularSuperhero {

  def superheroes(): Map[Int, String] = {

    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var names:Map[Int, String] = Map()

    val names_file = Source.fromFile("/Users/mntiwari/Udemy/SparkScala/Marvel-names.txt").getLines()

    names_file.foreach(line => {
      var fields = line.split("\"")

      if (fields.length>1) {
        names += (fields(0).trim.toInt -> fields(1))
      }

    })

    return names
  }

  def getHeroName(id: Int) (implicit superhero_names:Map[Int, String]): String = {
    superhero_names.getOrElse(id, "UNKNOWN")
  }

  def main(args: Array[String]) {

    val config = new SparkConf()
    config.setAppName("PopulatSuperhero").setMaster("local[*]")

    val sc = new SparkContext(config)

    // broadcast superhero names
    val superhero_names = sc.broadcast(superheroes())
    implicit val superheroeMap = superhero_names.value

    //load social file
    val lines = sc.textFile("/Users/mntiwari/Udemy/SparkScala/Marvel-graph.txt")

    val heroConnections = lines.map(l => {
      val fields = l.split("\\s+")
      (getHeroName(fields(0).toInt), fields.length-1)
    })

    val heroConnectionCount = heroConnections.reduceByKey( _+_ )

    val popularHero = heroConnectionCount.map( x => (x._2, x._1)).sortByKey(false)

    val result = popularHero.collect()

    result.foreach(println)
  }

}
