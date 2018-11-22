import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object analyzer {

  def main(args : Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[*]")
      .appName("Spark Log Analyzer")
      .getOrCreate;

    val logs = logsReader("access.log",spark)
    logs.show(5)

  }

  def logsReader(path:String,spark: SparkSession ) : DataFrame = {

    var logs = spark.read.option("delimiter"," - - ").text("src/main/resources/"+path)
    logs = logs
      .withColumn("Ip", split(col("value")," - - ").getItem(0))
      .withColumn("others", split(col("value")," - - ").getItem(1))
      .withColumn("time", split(col("others"),"\"").getItem(0))
      .withColumn("request", split(col("others"),"\"").getItem(1))
      .drop("value")
      .drop("others")

    val formatDate = udf((date : String) => date.substring(1,date.length-1))
    logs = logs.withColumn("time", formatDate(logs("time")) )

    val formatRequest = udf((request : String) => request.substring(0,request.length-1))
    logs = logs.withColumn("request", formatRequest(logs("request")) )

    return logs

  }


}
