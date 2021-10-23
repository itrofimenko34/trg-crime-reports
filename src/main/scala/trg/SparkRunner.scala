package trg

import org.apache.spark.sql.SparkSession

trait SparkRunner {
  val appName: String
  val coresNum: Int
  val logLevel: String

  lazy val spark: SparkSession = SparkSession.builder()
    .appName(s"trg-crime-reports-$appName")
    .config("spark.default.parallelism", coresNum * 2)
    .config("spark.sql.shuffle.partitions", coresNum * 2)
    .getOrCreate()

  def setLoggerLevel(): Unit = spark.sparkContext.setLogLevel(logLevel)
}
