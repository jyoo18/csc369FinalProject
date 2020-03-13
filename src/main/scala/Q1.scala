import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Q1 {
  def getRoadType(street: String) = {

    val interstate = "I-([0-9]+) ([NEWS])".r
    val usHighway = "US-([0-9]+) ([NEWS])".r
    val stateHighway = "([A-Z]{2})-([0-9]+) ([NEWS])".r
    val localFreeway = "(^|[ a-zA-Z]+)Fwy(\\z| [NEWS])".r
    val localHighway = "(^|[ a-zA-Z]+)Hwy(\\z| [NEWS])".r

    street match {
      case stateHighway(state, num, bound) => "State Highway"
      case interstate(num, bound) => "Interstate"
      case usHighway(num, bound) => "US Highway"
      case localFreeway(name, bound) => "Freeway"
      case localHighway(name, bound) => "Highway"
      case _ => "Local Street"
    }
  }

  def main(args: Array[String]) = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // [1]ID,[2]Source,[3]Severity,[4]Start_Time,[5]End_Time,[6]Start_Lat,[7]Start_Lng,[8]Description,[9]Number,
    // [10]Street,[11]Side,[12]City,[13]County,[14]State,[15]Temperature(F),[16]Humidity(%),[17]Visibility(mi),
    // [18]Wind_Speed(mph),[19]Precipitation(in)

    val file = sc.textFile("accidents.csv")
    val header = file.first()
    val accidentsByState = file.filter(_ != header).map(line =>
      (line.split(",")(14), line.split(",")(3).toDouble)
    ).persist() // (state, severity)

    // Which states have the highest number of accidents? What is the average severity of the accidents?
    accidentsByState.aggregateByKey((0.0, 0.0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map({case (state, (sum, count)) =>
      (state, (count, sum / count))
    }).sortBy({case (state, (count, avg)) =>
      -count
    }).collect().foreach({case (state, (sum, count)) =>
      println(f"$state  ${sum.toInt} $count%.2f")
    })

    // Which road types have the highest number of accidents? What is the average severity of the accidents by road type?
    val accidentsByRoad = file.filter(_ != header).map(line =>
      (getRoadType(line.split(",")(10)), line.split(",")(3).toDouble)
    ).persist() // (roadType, severity)

    accidentsByRoad.aggregateByKey((0.0, 0.0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).map({case (roadType, (sum, count)) =>
      (roadType, (count, sum / count))
    }).sortBy({case (roadType, (count, avg)) =>
      -count
    }).collect().foreach({case (roadType, (sum, count)) =>
      println(f"$roadType  ${sum.toInt} $count%.2f")
    })
  }
}
