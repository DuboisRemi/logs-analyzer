name := "logs-analyzer"

version := "0.1"

scalaVersion := "2.11.12"


//extra dependencies
val sparkVersion = "2.2.0"

//refer at https://mvnrepository.com
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0")
  .map(_.exclude("org.slf4j","log4j-over-slf4j")
)