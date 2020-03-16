import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import Q1.getRoadType

import plotly._
import element._
import layout._
import Plotly._

object Q4 {
  def analyze(sc: SparkContext, infile: String, is_road: Boolean): Unit = {
    val accidents_file = sc.textFile(infile)
    val header = accidents_file.first()
    // [1]ID,[2]Source,[3]Severity,[4]Start_Time,[5]End_Time,[6]Start_Lat,[7]Start_Lng,[8]Description,[9]Number,
    // [10]Street,[11]Side,[12]City,[13]County,[14]State,[15]Temperature(F),[16]Humidity(%),[17]Visibility(mi),
    // [18]Wind_Speed(mph),[19]Precipitation(in)
    val accidents = accidents_file
      .filter(_ != header)
      .map({
        (line) =>
          val l = line.split(",")
          if (is_road) {
            ((l(2), getRoadType(l(14))), 1)
          }
          else {
            ((l(2), l(4)), 1)
          }
      })

    // Reduce to the form ((reporting source, road type),# of incidents)
    val by_source_and_group = accidents
      .reduceByKey(_ + _)

    // Reduce to the form (reporting source,# of incidents)
    val by_source = by_source_and_group
      .map({
        case ((src,_),total) => (src,total)
      })
      .reduceByKey(_ + _)

    // Join the sub totals to the total to find the proportion of incidents
    // on each road type
    val distribution = by_source_and_group
      .cartesian(by_source)
      .filter({
        case (((src1,_),_),(src2,_)) => src1 == src2
      })
      .map({
        case (((src,group),subtotal),(_,total)) => ((src,group),subtotal.toDouble / total)
      })
      .collect

    // Show the output
    distribution
      .sorted
      .foreach(println)

    // Create separate bar graphs for each source and road type
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
              case ((_,group),prop) => (group,prop)
            })
            .sorted
          Bar(
            x = data.map(_._1).toList,
            y = data.map(_._2).toList,
            name = src
          )
      })

    // configure graph layout
    val aspect = 10.0 / 6
    val height = 800
    val width = (height * aspect).toInt
    val layout = Layout(
      barmode = BarMode.Group,
      height = height,
      width = width,
      title = if (is_road) "Percentage of Road Types by Source" else "Percentage of Severity by Source"
    )

    // Show the plot
    Plotly.plot(
      path = if (is_road) "src_rdway.html" else "src_severity.html",
      traces = bars,
      layout = layout,
      addSuffixIfExists = false,
      openInBrowser = false
    )
  }

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Usage: Q4 <infile>")

    val infile = args(0)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    analyze(sc, infile, true)
    analyze(sc, infile, false)
  }
}
