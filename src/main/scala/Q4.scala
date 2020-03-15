import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import Q1.getRoadType

import plotly._
import element._
import layout._
import Plotly._

object Q4 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val accidents_file = sc.textFile("big_accidents.csv")

    val header = accidents_file.first()

    // [1]ID,[2]Source,[3]Severity,[4]Start_Time,[5]End_Time,[6]Start_Lat,[7]Start_Lng,[8]Description,[9]Number,
    // [10]Street,[11]Side,[12]City,[13]County,[14]State,[15]Temperature(F),[16]Humidity(%),[17]Visibility(mi),
    // [18]Wind_Speed(mph),[19]Precipitation(in)
    val accidents = accidents_file
      .filter(_ != header)
      .map({
        (line) =>
          val l = line.split(",")
          ((l(2),getRoadType(l(14))),1)
      })

    val by_source_and_road = accidents
      .reduceByKey(_ + _)

    val by_source = by_source_and_road
      .map({
        case ((src,_),total) => (src,total)
      })
      .reduceByKey(_ + _)

    val distribution = by_source_and_road
      .cartesian(by_source)
      .filter({
        case (((src1,_),_),(src2,_)) => src1 == src2
      })
      .map({
        case (((src,road),subtotal),(_,total)) => ((src,road),subtotal.toDouble / total)
      })
      .collect

    distribution
      .sorted
      .foreach(println)

    val bars = by_source
      .map({
        case (src,_) => src
      })
      .distinct
      .collect
      .map({
        (src) =>
          val data = distribution
            .filter({
              case ((src1,_),_) => src == src1
            })
            .map({
              case ((_,road),prop) => (road,prop)
            })
          Bar(
            x = data.map(_._1).toList,
            y = data.map(_._2).toList,
            name = src
          )
      })

    val aspect = 10.0 / 6
    val height = 800
    val width = (height * aspect).toInt
    val layout = Layout(
      barmode = BarMode.Group,
      height = height,
      width = width,
      title = "Percentage of Road Types by Source"
    )

    Plotly.plot(
      path = "src_rdway.html",
      traces = bars,
      layout = layout,
      addSuffixIfExists = false,
      openInBrowser = false
    )
  }
}
