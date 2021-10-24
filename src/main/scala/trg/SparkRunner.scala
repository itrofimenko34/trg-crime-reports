package trg

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * Base class for all runners. Contains common parameters and functions.
 */
abstract class SparkRunner extends App with Logging {
  /**
   * Application name.
   */
  val appName: String

  /**
   * Arguments container.
   * Key - argument name
   * Value - argument value
   */
  val argsMap: Map[String, String] = parseArguments(args)

  /**
   * Number of cores to use. SparkSession property. Default: 2
   */
  protected val coresNum: Int = argsMap.get("coresNum").map(_.toInt).getOrElse(2)
  /**
   * Log level. Default: ERROR
   */
  protected val logLevel: String = argsMap.getOrElse("logLevel", "ERROR")
  /**
   * Master url. SparkSession property. Default: local[coresNum]
   */
  protected val master: String = argsMap.getOrElse("master", s"local[$coresNum]")

  /**
   * Configure and start SparkSession
   */
  lazy val spark: SparkSession = SparkSession.builder()
    .appName(s"trg-crime-reports-$appName")
    .master(master)
    .config("spark.default.parallelism", coresNum * 2)
    .config("spark.sql.shuffle.partitions", coresNum * 2)
    .getOrCreate()

  /**
   * Entry point of all applications
   */
  def run(): Unit = {
    spark.sparkContext.setLogLevel(logLevel)

    println(s"$appName Spark job has been started!")
    val startTime = System.currentTimeMillis

    process()

    println(s"Total processing time: ${(System.currentTimeMillis - startTime) / 1000D} seconds")
    println(s"Finished!")
  }

  /**
   * This method should contain runner's business logic.
   */
  def process(): Unit

  /**
   * Transforms application arguments formatted in key=value into a map type.
   * @param arguments - application arguments
   * @return
   */
  def parseArguments(arguments: Array[String]): Map[String, String] = {
    val argsSeparator: String = "="
    arguments
      .filter(_.contains(argsSeparator))
      .map { arg =>
        val splits = arg.split(argsSeparator)
        splits(0) -> splits(1)
      }.toMap
  }
}
