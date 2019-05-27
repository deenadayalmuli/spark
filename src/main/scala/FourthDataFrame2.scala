import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.Tuple2

class FourthDataFrame2 {

}

object FourthDataFrame2{
  //case class Person(Retailer:Int, Name:String, Street:String, City :String, state :String,  Zip :Int, Quick_Draw:String,Latitude:Float,Longitude:Float, Location_1:String)
 case class Person( Retailer:Int, Name:String, Street:String, City :String, state :String, Zip :Int )
  case class Person1(   City :String,  Zip :Int,Count :Int )

  def main(args : Array[String]): Unit ={
    try {
      val conf = new SparkConf().setMaster("local").setAppName("Deena4")
      val sc = new SparkContext(conf)

      val sqlContext = new SQLContext(sc)
      import sqlContext.implicits._


  val ret = sc.textFile("/Users/deena/Downloads/NYS_Lottery_Retailers1.csv").map(_.split(","))
    //      val ret = sc.textFile("/user/spark/NYS_Lottery_Retailers1.csv").map(_.split(","))
        .map(p => Person(p(0).trim.toInt, p(1), p(2), p(3),p(4),p(5).trim.toInt)).toDF()
      // println(ret.getClass)
      ret.persist(StorageLevel.MEMORY_ONLY)
      ret.registerTempTable("DeenaTest")

      val myquery1 = sqlContext.sql("select * from DeenaTest")

      //myquery1.show()
      val myquery2 = sqlContext.sql("select count(*) count from DeenaTest ")
     // myquery2.show()

      val myquery3 = sqlContext.sql("select City, count(*) count from DeenaTest group by  City")
     // myquery3.show()

      val myquery4 = sqlContext.sql("select City,Zip, count(*) count from DeenaTest group by  City,Zip")
     // myquery4.show()

      val myquery5 = sqlContext.sql("select distinct City  from DeenaTest group by  City,Zip")

   //     myquery5.coalesce(1).write.save("/user/spark/joboutput4/test.txt")
    //  myquery5.coalesce(1).write.format("com.databricks.spark.csv").option("header", "false").save("/user/spark/deena4/test.csv")

      /*
      myquery3.coalesce(1).write.save("/Users/deena/Downloads/sparkoutput/deena8.txt")
       myquery3.coalesce(1).write.format("com.databricks.spark.csv").option("header", "false").save("/Users/deena/Downloads/sparkoutput1/file8.csv")
      println(myquery3.getClass)
      println(myquery3.rdd.getNumPartitions)

      val ret1 = sc.textFile("/Users/deena/Downloads/sparkoutput1/file2.csv").map(_.split(","))
        .map(p => Person1(p(0), p(1).trim.toInt, p(2).trim.toInt)).toDF()
      ret1.show(16)
      */
    }
    catch{
      case a: NumberFormatException=> println("Number format Exception")

    }

  }
}