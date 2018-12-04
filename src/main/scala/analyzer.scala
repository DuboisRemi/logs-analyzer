import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import com.datastax.spark.connector.cql.CassandraConnectorConf

object analyzer {

  def main(args : Array[String]): Unit ={
    val t0 = System.currentTimeMillis()

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local[*]")
      .appName("Spark Log Analyzer")
      .getOrCreate
    spark.setCassandraConf("localCluster",CassandraConnectorConf.ConnectionHostParam.option("127.0.0.1"))

    val logs = logsParser(spark.read.option("delimiter", " - - ").text("src/main/resources/access.log"))

    //save logs in Cassandra Table
    println(logs.count())
    logs.write
      .cassandraFormat("logs","logs_spark","localCluster")
      .mode("overwrite")
      .option("confirm.truncate", true)
      .save()



    logs.show(5)
    logs.groupBy("request_method").count().show()
    logs.groupBy("type_request_status").count().show()
    logs.groupBy("request_status").count().show()
    countTimeAttendance(logs).show()
    countServerErrorsPerHour(logs).show()
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) + "ms")
    scala.io.StdIn.readLine("enter e to exit : \n")
    spark.stop()
  }

  /* Read a log fil in resources
   * load it and retrun it into a dataframe  */
  def logsParser(logs : DataFrame ) : DataFrame = {

    val parsedLogs = logs.withColumn("ip", split(col("value"), " - - ").getItem(0))
      .withColumn("others", split(col("value"), " - - ").getItem(1))
      .withColumn("time", split(col("others"), "\"").getItem(0))
      .withColumn("request", split(col("others"), "]").getItem(1))
      .drop("value")
      .drop("others")

    val formatedTimeLogs = logsFormatTime(parsedLogs)
    val formatedRequestLogs = logsFormatRequest(formatedTimeLogs)
    val typedRequestLogs = getTypeRequestStatus(formatedRequestLogs)
    typedRequestLogs.drop("request")
  }

  /*Split time into Date and Hour */
  def logsFormatTime(inputLogs : DataFrame) : DataFrame = {

    val formatDate = udf((date: String) => date.substring(1, date.length - 1))
    val getTime = udf((time: String) => time.substring(12, time.length() - 1))
    var logs = inputLogs.withColumn("time", formatDate(inputLogs("time")))
    logs = logs
      .withColumn("date", split(col("time"), ":").getItem(0))
      .withColumn("time", getTime(logs("time")))
    logs
  }

  //Split Request into multiple value
  def logsFormatRequest(inputLogs : DataFrame) : DataFrame = {
    val formatRequest = udf((request : String) => request.substring(2,request.length-5))
    var logs = inputLogs.withColumn("request", formatRequest(inputLogs("request")) )

    logs = logs
      .withColumn("request_method",split(col("request")," ").getItem(0))
      .withColumn("request_path",split(col("request")," ").getItem(1))
      .withColumn("request_protocol",split(col("request")," ").getItem(2))
      .withColumn("request_protocol",substring(col("request_protocol"),0,8))
      .withColumn("request_status",split(col("request")," ").getItem(3))
      .withColumn("request_content",split(col("request")," ").getItem(4))
      .withColumn("referrer_path",split(col("request")," ").getItem(5))

    logs = logs.filter(logs("request_status") =!= "null")

    logs = logs.filter(logs("request_method") =!= "T")
    logs = logs.withColumn("request_method",when(col("request_method")=== "Head", "HEAD")
              .otherwise(col("request_method")))
    logs
  }

  //Add a column with the type of the request status
  def getTypeRequestStatus(inputLogs : DataFrame) : DataFrame = {
    val typeRequest = udf((status : String)=>
      if (status.startsWith("2")) "Sucess"
      else if (status.startsWith("3")) "Redirection"
      else if (status.startsWith("4")) "Client error"
      else if (status.startsWith("5")) "Server error"
      else "Unknown Status")
    inputLogs.withColumn("type_request_status",typeRequest(col("request_status")))
    }

  //Count number of request by hour
  def countTimeAttendance(inputLogs : DataFrame) : DataFrame = {
    val logs = inputLogs.withColumn("hour",split(col("time"), ":").getItem(0))
    logs.groupBy("hour").count().orderBy(desc("count"))
  }

  //Count the number of server errors per hour
  def countServerErrorsPerHour(inputLogs :DataFrame) : DataFrame = {
    val errorsPerHour = inputLogs
      .filter(inputLogs("type_request_status") === "Server error")
      .withColumn("hour",split(col("time"), ":").getItem(0))
      .groupBy("hour").count().orderBy(desc("count"))
    errorsPerHour
  }
}
