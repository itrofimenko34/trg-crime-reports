package trg

import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, input_file_name, regexp_extract}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import trg.model.{CrimeOutcome, CrimeReport}

object ParquetExporter extends App with Logging with SparkRunner {

  val appName = "parquet-exporter"

  val inputPath: String = args(0)
  val outputPath: String = args(1)
  val coresNum: Int = args(2).toInt
  val logLevel: String = args(3)

  val districtNameExtractRegex: String = "/\\d{4}-\\d{2}-(\\w*)-street\\.csv$"

  setLoggerLevel()
  run(spark, inputPath, outputPath)

  def run(spark: SparkSession, inputFolder: String, outputFolder: String): Unit = {
    import spark.implicits._
    val startTime = System.currentTimeMillis

    println(s"ParquetExporter Spark job has been started!")

    val crimesPath = s"$inputFolder/*/*-street.csv"
    val crimesDS = readInputCSV(spark, crimesPath, CrimeReport.validationSchema)
      .map(CrimeReport.apply)

    val outcomesPath = s"$inputFolder/*/*-outcomes.csv"
    val outcomesDS = readInputCSV(spark, outcomesPath, CrimeOutcome.validationSchema)
      .map(CrimeOutcome.apply)

    val completeCrimesPartDS = crimesDS
      .joinWith(outcomesDS, crimesDS(CrimeReport.joinKey) === outcomesDS(CrimeOutcome.joinKey), joinType = "left")
      .map {
        case (crime, null) => crime
        case (crime, outcome) =>
          outcome.outcomeType.filterNot(crime.lastOutcome.contains)
            .map(newOutcome => crime.copy(lastOutcome = Option(newOutcome)))
            .getOrElse(crime)
      }

    completeCrimesPartDS.write.mode(saveMode = "overwrite").json(outputFolder)

    println(s"Total processing time: ${(System.currentTimeMillis - startTime) / 1000D} seconds")
    println(s"Output path: $outputFolder")
    println(s"Finished!")
  }

  def readInputCSV(spark: SparkSession, path: String, validationSchema: StructType): DataFrame = {
    val inputDataframe = spark.read
        .option("header", "true")
        .csv(path)

    val dfWithDistrictName = addDistrictName(inputDataframe)

    if(validateDFSchema(dfWithDistrictName, validationSchema)) {
      logDebug(s"Dataframe has passed schema validation. Input path: $path")
      dfWithDistrictName
    } else {
      logError(s"Dataframe schema:")
      logError(dfWithDistrictName.schema.toString)
      logError(s"Validation schema:")
      logError(validationSchema.toString)
      throw new Exception("Input data doesn't match required schema!")
    }
  }

  def validateDFSchema(dataFrame: DataFrame, validationSchema: StructType): Boolean = {
    validationSchema.forall(dataFrame.schema.contains)
  }

  def addDistrictName(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("fileName", input_file_name())
      .withColumn(CrimeReport.districtNameLabel, regexp_extract(col("fileName"), districtNameExtractRegex, 1))
  }
}
