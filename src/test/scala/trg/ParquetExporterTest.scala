package trg

import org.scalatest.funsuite.AnyFunSuite
import trg.model.{CrimeOutcome, CrimeReport}

class ParquetExporterTest extends AnyFunSuite with SparkTestSpec {
  val streetTestPath = "./src/test/resources/data/*-street.csv"
  val outcomesTestPath = "./src/test/resources/data/*-outcomes.csv"

  test("ParquetExporter should add districtName") {
    val expectedDistrictName = "test"

    val inputDF = spark.read.option("header", "true").csv(streetTestPath)

    ParquetExporter.addDistrictName(inputDF)
      .select("filename", "districtName").show(20, truncate = false)

    val nonEmptyDistrictNameCount = ParquetExporter.addDistrictName(inputDF)
      .filter(s"${CrimeReport.districtNameLabel} = '$expectedDistrictName'")
      .count()

    val expectedCount = inputDF.count()
    assert(expectedCount == nonEmptyDistrictNameCount)
  }

  test("ParquetExporter should validate input dataframe schema") {

    val streetDF = ParquetExporter.addDistrictName(spark.read.option("header", "true").csv(streetTestPath))
    val outcomesDF = spark.read.option("header", "true").csv(outcomesTestPath)

    assert(ParquetExporter.validateDFSchema(streetDF, CrimeReport.validationSchema))
    assert(ParquetExporter.validateDFSchema(outcomesDF, CrimeOutcome.validationSchema))
  }
}
