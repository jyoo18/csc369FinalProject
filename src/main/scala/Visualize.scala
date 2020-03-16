import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

import plotly._
import element._
import layout._
import Plotly._

object Visualize {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // [1]ID,[2]Source,[3]Severity,[4]Start_Time,[5]End_Time,[6]Start_Lat,[7]Start_Lng,[8]Description,[9]Number,
    // [10]Street,[11]Side,[12]City,[13]County,[14]State,[15]Temperature(F),[16]Humidity(%),[17]Visibility(mi),
    // [18]Wind_Speed(mph),[19]Precipitation(in)
    val accidents_file = sc.textFile("big_accidents.csv")

    val header = accidents_file.first()

    val accidents = accidents_file
      .filter(_ != header)
      .map({
        (line) =>
          val l = line.split(",")
          ((l(8).toDouble,l(7).toDouble),l(4).toInt)
      })
      .collect

    val longs = accidents.map(_._1._1).toList
    val lats = accidents.map(_._1._2).toList
    val severity_color = accidents
      .map(_._2)
      .map({
        case 0 => Color.RGB(0, 0, 0)
        case 1 => Color.RGB(0, 0, 0)
        case 2 => Color.RGB(51, 255, 51)
        case 3 => Color.RGB(255, 255, 51)
        case 4 => Color.RGB(255, 51, 51)
      })
      .toList
    val severity_size = accidents.
      map(_._2 + 2)
      .toList

    val plot = Scatter(
      longs,
      lats,
      mode = ScatterMode(ScatterMode.Markers),
      marker = Marker(
        symbol = Symbol.Circle(),
        size = severity_size,
        color = severity_color
      )
    )

    val height = 900
    val width = (height * 1.40946).toInt
    plot.plot(
      height = height,
      width = width,
      addSuffixIfExists = false,
      openInBrowser = false,
      title = "Location and Severity of Accidents",
      path = "map.html"
    )
  }
}
