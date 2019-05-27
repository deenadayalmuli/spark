import org.apache.spark.{SparkConf, SparkContext}

class FifthS3ToLocalar {

}

object FifthS3ToLocalar {

  def main(args : Array[String]): Unit = {

    // configurations and context
    val conf = new SparkConf().setMaster("local").setAppName("Deena2");
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "AKIAJJU2FHN2IWCX3MEQ")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "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")
    // configurations and context

    // Load data to s3 from local
    val localRDD = sc.textFile("/Users/deena/Desktop/jsons/NYS_Lottery_Retailers.csv")
     localRDD.saveAsTextFile("s3n://deenabkt/deena/s3inputfile.csv")


    // Load data from s3 to local
    val s3RDD = sc.textFile("s3n://deenabkt/deena/s3inputfile.csv")
     s3RDD.saveAsTextFile("/Users/deena/Desktop/aws2")

  }

}
