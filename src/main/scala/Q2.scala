import org.apache.spark.SparkContext._
import scala.io._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

import java.io._

object Q2 {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val accidents = sc.textFile("accidents.csv")

    // Gather data into 4 RDDs:
    // - All
    // - Records where description starts with "Accident on..."
    // - Records where description follows pattern "Lane/road blocked/obstructed due to accident..."
    // - Everything else
    var AllAccidentRDD = accidents.filter(x => x.split(",")(0).length() > 0).map(x => (x.split(",")(0).toInt, (x.split(",")(2), x.split(",")(3).toDouble))).persist()
    var AccidentOnRDD = accidents.filter(x => x.split(",")(8).startsWith("Accident on")).map(x => (x.split(",")(0).toInt, (x.split(",")(2), x.split(",")(3).toDouble))).persist()
    var LaneBlockedRDD = accidents.filter(x => x.split(",")(8).contains("due to")).map(x => (x.split(",")(0).toInt, (x.split(",")(2), x.split(",")(3).toDouble))).persist()
    var OtherRDD = AllAccidentRDD.subtractByKey(AccidentOnRDD).subtractByKey(LaneBlockedRDD).persist()

    // Split data based on which API was used to gather it
    var AccidentOnMapQuestRDD = AccidentOnRDD.filter(x => x._2._1.equals("MapQuest")).map(x => (x._1, x._2._2)).persist()
    var AccidentOnBingRDD = AccidentOnRDD.filter(x => !x._2._1.equals("MapQuest")).map(x => (x._1, x._2._2)).persist()
    var LaneBlockedMapQuestRDD = LaneBlockedRDD.filter(x => x._2._1.equals("MapQuest")).map(x => (x._1, x._2._2)).persist()
    var LaneBlockedBingRDD = LaneBlockedRDD.filter(x => !x._2._1.equals("MapQuest")).map(x => (x._1, x._2._2)).persist()
    var OtherMapQuestRDD = OtherRDD.filter(x => x._2._1.equals("MapQuest")).map(x => (x._1, x._2._2)).persist()
    var OtherBingRDD = OtherRDD.filter(x => !x._2._1.equals("MapQuest")).map(x => (x._1, x._2._2)).persist()
    
    // Print average severity and number of records per RDD
    println("AllAccidentRDD: " + computeSeverityAvg(AllAccidentRDD.map(x => (x._1, x._2._2))) + " count: " + AllAccidentRDD.count())
    println("AccidentOnMapQuestRDD: " + computeSeverityAvg(AccidentOnMapQuestRDD) + " count: " + AccidentOnMapQuestRDD.count())
    println("AccidentOnBingRDD: " + computeSeverityAvg(AccidentOnBingRDD) + " count: " + AccidentOnBingRDD.count())
    println("LaneBlockedMapQuestRDD: " + computeSeverityAvg(LaneBlockedMapQuestRDD) + " count: " + LaneBlockedMapQuestRDD.count())
    println("LaneBlockedBingRDD: " + computeSeverityAvg(LaneBlockedBingRDD) + " count: " + LaneBlockedBingRDD.count())
    println("OtherMapQuestRDD: " + computeSeverityAvg(OtherMapQuestRDD) + " count: " + OtherMapQuestRDD.count())
    println("OtherBingRDD: " + computeSeverityAvg(OtherBingRDD) + " count: " + OtherBingRDD.count())
  }

  def computeSeverityAvg(rdd: RDD[(Int, Double)]) : Double = {
    val result = rdd.aggregate((0.0, 0.0))(
      (acc, idAndSeverity) => (acc._1 + idAndSeverity._2, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    return result._1 * 1.0 / result._2
  }
}
