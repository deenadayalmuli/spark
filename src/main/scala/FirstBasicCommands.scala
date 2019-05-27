

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class FirstBasicCommands {

}

  object FirstBasicCommands {

    def main(args: Array[String] )
    {

      val conf = new SparkConf().setAppName("Deena").setMaster("local")
      val sc = new SparkContext(conf)

      val data = Array(1, 2, 3, 4, 5)
      val distData = sc.parallelize(data)
      println("Test" + distData.getClass)
    val lines = sc.textFile("/Users/deena/Desktop/jsons/NYS_Lottery_Retailers.csv")
      //val lines = sc.textFile("/user/spark/SFPD_Incidents_from_1_January_2003.csv")
       // val lines = sc.textFile("/user/spark/NYS_Lottery_Retailers.csv")

      //val lines = sc.textFile("/home/cloudera/Desktop/test_deena/NYS_Lottery_Retailers.csv")
      println(lines.getClass)
      println(lines.take(5).foreach {
        println
      })

      val lineLengths = lines.map(s => s.length)
      println(lineLengths.getClass)
      println(lineLengths.take(5).foreach {
        println
      })
      val totalLength = lineLengths.reduce((a, b) => a + b)
      println(totalLength)

    }
  }