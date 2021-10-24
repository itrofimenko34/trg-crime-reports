package trg

import org.apache.spark.sql.functions.{input_file_name, regexp_extract}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import trg.model.{CrimeOutcome, CrimeReport}

/**
 * Runner for exporting crimes and outcomes csv files into a parquet format.
 */
object ParquetExporter extends SparkRunner {
  override val appName = "parquet-exporter"

  // launch the processing
  run()

  /**
   * Contains processing logic of ParquetExporter.
   * Steps:
   *   0) Check and read input arguments
   *   1) Read crimes csv files
   *   2) Read outcomes csv files
   *   3) Join crimes and outcomes dataset and define last crime outcome
   *   4) Persist result in parquet
   */
  override def process(): Unit = {
    import spark.implicits._

    val inputPath: String = argsMap
      .getOrElse("inputPath", throw new Exception("Required argument 'inputPath' is not specified!"))

    val outputPath: String = s"$inputPath/parquet"

    val crimesDS = readCrimes(inputPath)
    val outcomesDS = readOutcomes(inputPath)

    // used as a substitute for null values
    val emptyValue = "None"

    val completeCrimesPartDS = crimesDS
      .joinWith(outcomesDS, crimesDS(CrimeReport.joinKey) === outcomesDS(CrimeOutcome.joinKey), joinType = "left")
      .map {
        case (crime, null) => crime
        case (crime, outcome) =>
          outcome.outcomeType.filterNot(crime.lastOutcome.contains)
            .map(newOutcome => crime.copy(lastOutcome = Option(newOutcome)))
            .getOrElse(crime)
      }.na.fill(emptyValue)

    completeCrimesPartDS.write.mode(saveMode = "overwrite").json(outputPath)

    println(s"Result is persisted in parquet at: $outputPath")
  }

  /**
   * Reads csv files, validates the schema and prepares Crimes Dataset
   * @param path - path to the input directory
   * @return crimes dataset
   */
  def readCrimes(path: String): Dataset[CrimeReport] = {
    import spark.implicits._
    val crimesPath = s"$path/*/*-street.csv"

    val crimesDF = readInputCSV(crimesPath)
    val validatedCrimesDF = validateInputDataframeSchema(crimesDF, CrimeReport.validationSchema)

    validatedCrimesDF.map(CrimeReport.apply)
  }

  /**
   * Reads csv files, validates the schema and prepares Outcomes Dataset
   * @param path - path to the input directory
   * @return outcomes dataset
   */
  def readOutcomes(path: String): Dataset[CrimeOutcome] = {
    import spark.implicits._
    val outcomesPath = s"$path/*/*-outcomes.csv"

    val outcomesDF = readInputCSV(outcomesPath)
    val validatedOutcomesDF = validateInputDataframeSchema(outcomesDF, CrimeOutcome.validationSchema)

    validatedOutcomesDF.map(CrimeOutcome.apply)
  }

  /**
   * Reads input csv data and add districtName column
   * @param path - csv files input path to load
   * @return
   */
  def readInputCSV(path: String): DataFrame = {
    val inputDataframe = spark.read
        .option("header", "true")
        .csv(path)

    addDistrictName(inputDataframe)
  }

  /**
   * Validates dataframe schema
   * @param dataframe - dataframe to check
   * @param validationSchema - validation schema
   * @return original dataframe
   */
  def validateInputDataframeSchema(dataframe: DataFrame, validationSchema: StructType): DataFrame = {
    if(validateDFSchema(dataframe, validationSchema)) {
      logDebug(s"Dataframe has passed schema validation.")
      dataframe
    } else {
      logError(s"Dataframe schema:")
      logError(dataframe.schema.toString)
      logError(s"Validation schema:")
      logError(validationSchema.toString)
      throw new Exception("Input data doesn't match required schema!")
    }
  }

  /**
   * Check the dataframe schema, to make sure it could be converted to the Dataset safely.
   * @param dataFrame - input dataframe
   * @param validationSchema - validation schema
   * @return true - when the dataframe contains all fields from validationSchema, otherwise - false
   */
  def validateDFSchema(dataFrame: DataFrame, validationSchema: StructType): Boolean = {
    validationSchema.forall(dataFrame.schema.contains)
  }

  /**
   * Takes districtName from fileName and adds it to the dataframe.
   * @param dataFrame - input dataframe
   * @return - original dataframe with districtName column
   */
  def addDistrictName(dataFrame: DataFrame): DataFrame = {
    val districtNameExtractRegex: String = "\\d{4}-\\d{2}-(\\w*)-street\\.csv$"
    dataFrame
      .withColumn(CrimeReport.districtNameLabel, regexp_extract(input_file_name(), districtNameExtractRegex, 1))
  }
}
