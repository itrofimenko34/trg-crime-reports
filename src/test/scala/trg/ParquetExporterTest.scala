package trg

import org.scalatest.funsuite.AnyFunSuite
import trg.model.{CrimeOutcome, CrimeReport}

class ParquetExporterTest extends AnyFunSuite with SparkTestSpec {

  val testResourcesPath = "./src/test/resources/data"
  val streetTestPath = s"$testResourcesPath/*/*-street.csv"
  val outcomesTestPath = s"$testResourcesPath/*/*-outcomes.csv"

  test("ParquetExporter should add districtName") {
    val expectedDistrictName = "test-district"

    val inputDF = spark.read.option("header", "true").csv(streetTestPath)

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

  test("ParquetExporter should prepare crimes dataset") {
    val crimesDS = ParquetExporter.readCrimes(testResourcesPath, spark)

    assert(crimesDS.count == 9)
  }

  test("ParquetExporter should prepare outcomes dataset") {
    val outcomesDS = ParquetExporter.readOutcomes(testResourcesPath, spark)

    assert(outcomesDS.count == 5)
  }

  test("ParquetExporter should define the lastOutcome"){
    import spark.implicits._

    val crimesDS = ParquetExporter.readCrimes(testResourcesPath, spark)
    val outcomesDS = ParquetExporter.readOutcomes(testResourcesPath, spark)

    val resDs = ParquetExporter.defineLastOutcome(crimesDS, outcomesDS, spark)

    // crime id that should be updated after joining with outcomes ds
    val crimeId = "b69c56a0dff28a76b265590bf64061dff7ecc25a018ec6aaa592817724011121"

    val crime = crimesDS.filter(_.crimeId.contains(crimeId)).collect().head
    val outcome = outcomesDS.filter(_.crimeId.contains(crimeId)).collect().head
    val resultCrime = resDs.filter(_.crimeId.contains(crimeId)).collect().head

    assert(outcome.outcomeType == resultCrime.lastOutcome && resultCrime.lastOutcome != crime.lastOutcome)
  }
}
