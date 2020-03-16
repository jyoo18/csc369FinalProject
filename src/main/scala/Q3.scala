package csc369FinalProject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Q3 {
  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val accidents = sc.textFile("input/accidents.csv")

    val header = accidents.first()
    // filter accidents before 2017 since data doesn't start at beginning of year (Feb 2016 as opposed to Jan (skews data w/o all months))
    val filteredAccidents = accidents.filter(line => line != header)
      .filter(line => line.split(",")(4).substring(0, 4).toInt > 2016).persist()
    val severeAccidents = filteredAccidents.filter(line => line.split(",")(3).toDouble > 3).persist()
    val numAccidents = filteredAccidents.count() // total accidents
    val avgAccidents = numAccidents / (365 * 2)  // average number of accidents per day

    // Severity = line(3)       ex) 3.0
    // Time of crash = line(4)  ex) 2016-12-19 15:32:43

    println("total accidents: " + numAccidents)
    println("average number of accidents per day: " + avgAccidents)

    println("\nWhat days of the year have the most crashes?" +
            "\n(month-day, number of times over the average)")
    filteredAccidents.map(line => (line.split(",")(4).substring(5, 10), 1.0))
      // (month-day, 1)
      .reduceByKey(_ + _)
      .mapValues(v => BigDecimal(v / avgAccidents).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)
      .sortBy(p => p._2, ascending = false)
      .collect()
      .take(10)
      .foreach(println)
    // these dates all have something in common:
    //    all are late fall or early winter
    //    indicative of first snowfall and beginning of holiday seasons
    //      - many driving to friends/family or shopping for presents but in challenging conditions


    /*
    // ------ alternate and more complicated way of finding the same info as above ------
    filteredAccidents.map({line =>
        val splits = line.split(",")
        // (month-day, (severity, 1))
        (splits(4).substring(5, 10), (splits(3).toDouble, 1))
      })
      .groupByKey()
      .mapValues({v =>
        v.toList
          .aggregate((0.0, 0.0))(
            (x, y) => (x._1 + y._1, x._2 + 1),
            (x, y) => (x._1 + y._1, x._2 + y._2))
      })
      .mapValues(v => v._2 / avgAccidents) // number of times over the avg
      .sortBy(p => p._2, ascending = false)
      .collect()
      .take(10)
      .foreach(println)
    // ------ ------ ------
    */

    println("\nWhat hours have the most accidents?" +
            "\n(hour, # of accidents)")
    filteredAccidents.map(line => (line.split(",")(4).substring(11, 13).toInt, 1))
      // (hour, 1)
      .reduceByKey(_ + _)
      .sortBy(p => p._2, ascending = false)
      .collect()
      .take(5)
      .foreach(println)
    // hours with most crashes are 8am and 7am followed by 5pm and 4pm
    // possible correlation: more people going to and from work = more drivers = peak traffic = more chances of an accident

    println("\nWhat hours have most accidents with severity at 4?" +
            "\n(hour, # of accidents)")

    severeAccidents.map({line =>
      val splits = line.split(",")
      // (hour, 1)
      (splits(4).substring(11, 13).toInt, 1)
    })
      .reduceByKey(_ + _)
      .sortBy(p => p._2, ascending = false)
      .collect()
      .take(5)
      .foreach(println)
    // 6 am has highest number of level 4 severities
    // possible correlation: traffic is not a peak yet and people speed leading to more severe crashes
  }
}
