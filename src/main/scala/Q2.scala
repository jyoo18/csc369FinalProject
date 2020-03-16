import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

import java.io._
import java.util._

object Q2 {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val accidents = sc.textFile("input/extracted.csv")
    val titleLine = accidents.first()

    // Gather data into 7 RDDs:
    // - All
    // - MapQuest RDDs
    //    - Records where description starts with "Accident on..."
    //    - Records where description follows pattern "Lane/road blocked/obstructed due to accident..."
    //    - Other
    // - Bing RDDs
    //    - Records where description has "Accident" at the end
    //    - Records where description has "Road closed due to accident" at the end
    //    - Other
    var AllAccidentRDD = accidents.filter(x => x != titleLine).map(x => (x.split(",")(0), x.split(",")(3).toDouble)).persist()
    var AccidentOnMapQuestRDD = accidents.filter(x => x.split(",")(1).contains("MapQuest") && x.split(",")(11).startsWith("Accident on")).map(x => (x.split(",")(0), x.split(",")(3).toDouble)).persist()
    var LaneBlockedMapQuestRDD = accidents.filter(x => x.split(",")(1).contains("MapQuest") && x.split(",")(11).contains("due to")).map(x => (x.split(",")(0), x.split(",")(3).toDouble)).persist()
    var OtherMapQuestRDD = accidents.filter(x => x.split(",")(1).contains("MapQuest")).map(x => (x.split(",")(0), x.split(",")(3).toDouble)).subtractByKey(AccidentOnMapQuestRDD).subtractByKey(LaneBlockedMapQuestRDD)
    var AccidentBingRDD = accidents.filter(x => x.split(",")(1).equals("Bing") && x.split(",")(11).contains("Accident")).map(x => (x.split(",")(0), x.split(",")(3).toDouble)).persist()
    var RoadClosedBingRDD = accidents.filter(x => x.split(",")(1).equals("Bing") && x.split(",")(11).contains("Road closed due to accident")).map(x => (x.split(",")(0), x.split(",")(3).toDouble)).persist()
    var OtherBingRDD = accidents.filter(x => x.split(",")(1).equals("Bing")).map(x => (x.split(",")(0), x.split(",")(3).toDouble)).subtractByKey(AccidentBingRDD).subtractByKey(RoadClosedBingRDD)

    // Print average severity and number of records per RDD
    println("AllAccidentRDD: " + computeSeverityAvg(AllAccidentRDD.map(x => (x._1, x._2))) + " count: " + AllAccidentRDD.count())
    println("AccidentOnMapQuestRDD: " + computeSeverityAvg(AccidentOnMapQuestRDD) + " count: " + AccidentOnMapQuestRDD.count())
    println("LaneBlockedMapQuestRDD: " + computeSeverityAvg(LaneBlockedMapQuestRDD) + " count: " + LaneBlockedMapQuestRDD.count())
    println("OtherMapQuestRDD: " + computeSeverityAvg(OtherMapQuestRDD) + " count: " + OtherMapQuestRDD.count())
    println("AccidentBingRDD: " + computeSeverityAvg(AccidentBingRDD) + " count: " + AccidentBingRDD.count())
    println("RoadClosedBingRDD: " + computeSeverityAvg(RoadClosedBingRDD) + " count: " + RoadClosedBingRDD.count())
    println("OtherBingRDD: " + computeSeverityAvg(OtherBingRDD) + " count: " + OtherBingRDD.count())
  }

  def computeSeverityAvg(rdd: RDD[(String, Double)]) : Double = {
    val result = rdd.aggregate((0.0, 0.0))(
      (acc, idAndSeverity) => (acc._1 + idAndSeverity._2, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    return result._1 * 1.0 / result._2
  }
}
