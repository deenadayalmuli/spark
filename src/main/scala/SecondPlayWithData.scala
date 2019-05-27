import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

class SecondPlayWithData {

}

object SecondPlayWithData{

  def main(args:Array[String]): Unit ={

    val conf = new SparkConf().setMaster("local").setAppName("Deena2");
    val sc = new SparkContext(conf)
    // val lines = sc.textFile("/Users/deena/Downloads/NYS_Lottery_Retailers.csv")

    val lines = sc.textFile("/user/spark/SFPD_Incidents_from_1_January_2003.csv")
    val words = lines.flatMap(  _.split(","))
    val pairs = words.map(a=>(a,1))
    println(pairs.getClass)
    pairs take(5) foreach{a=> println(a._1 +"==>"+a._2 )}

    val count = pairs.reduceByKey((a,b)=>a+b)
    count.persist(StorageLevel.MEMORY_ONLY)
    println(count.getClass)
    println(count.take(5).foreach(println))

    val swap_data = count.map(_.swap)
    val   sorted_data = swap_data.sortByKey(false,1)
    println("final data==>"+sorted_data.take(4).foreach(println))
  }


}