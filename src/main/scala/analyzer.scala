import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object analyzer {

  def main(args : Array[String]): Unit ={
    val t0 = System.currentTimeMillis()

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[*]")
      .appName("Spark Log Analyzer")
      .getOrCreate

    val logs = logsReader("access.log",spark)
    logs.show(5)
    logs.groupBy("request method").count().show()
    logs.groupBy("type request status").count().show()
    logs.groupBy("request status").count().show()
    logs.groupBy("time zone").count().show()
    countTimeAttendance(logs).show()
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    scala.io.StdIn.readLine("enter e to exit : \n")
    spark.stop()
  }
  /* Read a log fil in resources
   * load it and retrun it into a dataframe  */
  def logsReader(path:String,spark: SparkSession ) : DataFrame = {

    var logs = spark.read.option("delimiter", " - - ").text("src/main/resources/" + path)
    logs = logs
      .withColumn("Ip", split(col("value"), " - - ").getItem(0))
      .withColumn("others", split(col("value"), " - - ").getItem(1))
      .withColumn("time", split(col("others"), "\"").getItem(0))
      .withColumn("request", split(col("others"), "]").getItem(1))
      .drop("value")
      .drop("others")

    logs = logsFormatTime(logs)
    logs = logsFormatRequest(logs)
    logs = getTypeRequestStatus(logs)
    logs
  }

  /*Split time into Date and Hour */
  def logsFormatTime(inputLogs : DataFrame) : DataFrame = {

    val formatDate = udf((date: String) => date.substring(1, date.length - 1))
    val getTime = udf((time: String) => time.substring(12, time.length() - 1))
    var logs = inputLogs.withColumn("time", formatDate(inputLogs("time")))
    logs = logs
      .withColumn("date", split(col("time"), ":").getItem(0))
      .withColumn("time", getTime(logs("time")))
      .withColumn("time zone", split(col("time")," ").getItem(1))
    logs
  }

  //Split Request into multiple value
  def logsFormatRequest(inputLogs : DataFrame) : DataFrame = {
    val formatRequest = udf((request : String) => request.substring(2,request.length-5))
    var logs = inputLogs.withColumn("request", formatRequest(inputLogs("request")) )

    logs = logs
      .withColumn("request method",split(col("request")," ").getItem(0))
      .withColumn("request path",split(col("request")," ").getItem(1))
      .withColumn("request protocol",split(col("request")," ").getItem(2))
      .withColumn("request protocol",substring(col("request protocol"),0,8))
      .withColumn("request status",split(col("request")," ").getItem(3))
      .withColumn("request content",split(col("request")," ").getItem(4))
      .withColumn("referrer path",split(col("request")," ").getItem(5))

    logs = logs.filter(logs("request status") =!= "null")

    logs = logs.filter(logs("request method") =!= "T")
    logs = logs.withColumn("request method",when(col("request method")=== "Head", "HEAD")
              .otherwise(col("request method")))
    logs
  }

  def getTypeRequestStatus(inputLogs : DataFrame) : DataFrame = {
    val typeRequest = udf((status : String)=>
      if (status.startsWith("2")) "Sucess"
      else if (status.startsWith("3")) "Redirection"
      else if (status.startsWith("4")) "Client error"
      else if (status.startsWith("5")) "Server error"
      else "Unknown Status")
    inputLogs.withColumn("type request status",typeRequest(col("request status")))
    }

  def countTimeAttendance(inputLogs : DataFrame) : DataFrame = {
    var logs = inputLogs.withColumn("hour",split(col("time"), ":").getItem(0))
    logs.groupBy("hour").count().orderBy(desc("count"))
  }
}
