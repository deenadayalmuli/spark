import org.apache.spark.{SparkConf, SparkContext}

class SixthReadWebsite {

}



object SixthReadWebsite {


  def main(args: Array[String]): Unit ={

    val conf = new SparkConf().setMaster("local").setAppName("Deena2");
    val sc = new SparkContext(conf)
    val html = scala.io.Source.fromURL("https://spark.apache.org/").mkString
    val list = html.split("\n").filter(_ != "")
    val rdds = sc.parallelize(list)
    val count = rdds.filter(_.contains("Spark")).count()
println("Count is "+count)
  }

}