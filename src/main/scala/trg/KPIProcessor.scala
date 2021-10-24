package trg

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import trg.model.kpi.{Count, KPIParameters, Percent}

import scala.util.Try

/**
 * Runner for calculating crime KPIs
 */
object KPIProcessor extends SparkRunner {
  override val appName = "kpi-processor"

  // commonly used dataframe columns
  val countLabel: String = "count"
  val percentLabel: String = "percent"

  // launch the processing
  run()

  /**
   * This method contains processing logic of the KPIProcessor.
   * Steps:
   *   0) Check and read input arguments
   *   1) Read crimes parquet files
   *   2) Calculating required KPI
   *   4) Show result in JSON format
   */
  override def process(): Unit = {
    val kpiParameters = KPIParameters(argsMap)

    val crimesDS = readInputData(kpiParameters.inputPath)

    val (resultDS, sortCols) = kpiParameters.kpiType match {
      case Count => (countKPIs(crimesDS, kpiParameters.groupKeys), countLabel :: Nil)
      case Percent => (percentKPIs(crimesDS, kpiParameters.groupKeys), percentLabel :: Nil)
    }

    resultDS
      .sort(sortCols.map(kpiParameters.ordering): _*)
      .show(kpiParameters.limit, truncate = false)

    // print json line per row instead of spark format
//      .select(to_json(struct(resultDS.columns.map(col): _*)).as("json_KPIs"))
//      .limit(limit)
//      .collect()
//      .foreach(row => println(row.getAs[String]("json_KPIs")))
  }

  /**
   * Reads input data from parquet files
   * @param path - input parquet path
   * @return dataframe
   */
  def readInputData(path: String): DataFrame = {
    val inputPath = s"$path/*.json"

    Try {
     spark.read.json(inputPath)
    }.recover {
      case exc: Exception =>
        logError(s"Error reading input data from: $inputPath")
        throw exc
    }.get
  }

  /**
   * Calculate count KPIs
   * @param dataset - input dataset
   * @param groupKeys - columns set to group the data
   * @return kpi dataset
   */
  def countKPIs(dataset: DataFrame, groupKeys: Option[Array[Column]]): DataFrame = {
    groupKeys.fold {
      dataset.agg(count("*").as(countLabel))
    } { keys =>
      dataset.groupBy(keys: _*)
        .agg(count("*").as(countLabel))
    }
  }

  /**
   * Calculate percent KPIs
   * @param dataset - input dataset
   * @param groupKeys - columns set to group the data
   * @return kpi dataset
   */
  def percentKPIs(dataset: DataFrame, groupKeys: Option[Array[Column]]): DataFrame = {
    val sizeLabel = "size"

    dataset.persist

    val totalSize = dataset.count

    countKPIs(dataset, groupKeys)
      .withColumn(sizeLabel, lit(totalSize))
      .withColumn(percentLabel, round(col(countLabel) / col(sizeLabel) * 100, 1))
      .drop(sizeLabel, countLabel)
  }
}
