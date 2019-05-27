import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

class ThirdDataFrame {

}


object ThirdDataFrame{

  def main(args: Array[String]): Unit ={
    val conf = new SparkConf().setAppName("Deena2").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //  val df = sqlContext.read.json("/Users/deena/Desktop/filter/emp.json")
   val df = sqlContext.read.json("/user/spark/emp.json")

    df.show()

    df.printSchema()
    df.select("name").show
    df.select(df("name"),df("age")+11).show
    df.filter(df("age")>35).show
  }
}