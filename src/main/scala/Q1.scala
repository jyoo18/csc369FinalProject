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

    // Which states have the highest rate of accidents?
    val accidentsByState = file.filter(_ != header).map(line =>
      (line.split(",")(14), 1)
    ).persist() // (state, 1)

    val topStates = accidentsByState.reduceByKey((x, y) =>
      x + y
    ).sortBy({case (state, count) =>
      -count
    })

    // For each state, which roads have the highest rate of accidents? What is the average severity for each road type?
    val accidentsByRoad = file.filter(_ != header).map(line =>
      (line.split(",")(14), (getRoadType(line.split(",")(10)), line.split(",")(3).toDouble))
    ).persist() // (state, (roadType, severity))

    val topRoads = accidentsByRoad.groupByKey().map({case (state, roadTypes) =>
      val severityByRoad = roadTypes.groupBy({case (roadType, severity) =>
        roadType
      }).map({case (roadType, roadMap) =>
        val sumCount = roadMap.aggregate((0.0, 0))(
          (acc, value) => (acc._1 + value._2, acc._2 + 1),
          (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
        )
        (roadType, sumCount._2, sumCount._1 / sumCount._2.toDouble)
      }).toList.sortBy({case (roadType, count, sev) => -count})

      (state, severityByRoad)
    }).persist()

    // For the top 5 states with the most accidents, which types of roads do most of them occur and how severe are they?
    topStates.join(topRoads).sortBy({case (state, (count, roadTypes)) =>
      -count
    }).take(5).foreach({case (state, (count, roadTypes)) =>
      var result = state + " " + count
      roadTypes.foreach({case (roadType, count, sev) =>
        result = result + f", ($roadType, $count, $sev%.2f)"
      })

      println(result)
    })

  }
}
