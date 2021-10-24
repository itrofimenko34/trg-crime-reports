package trg.model

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.scalatest.funsuite.AnyFunSuite

class CrimeReportTest extends AnyFunSuite {

  test("CrimeReport should properly convert valid rows") {
    val id = "id1"
    val districtName = "districtName1"
    val latitude = 50.0
    val longitude = 50.0
    val crimeType = "crimeType1"
    val lastOutcome = "lastOutcome1"

    val validData: List[Array[Any]] = List(
      Array(id, districtName, latitude.toString, longitude.toString, crimeType, lastOutcome),
      Array(null, "districtName2", null, null, null, null)
    )

    val validRows = validData.map(arr => new GenericRowWithSchema(arr, CrimeReport.validationSchema))

    val crimes = validRows.map(CrimeReport.apply)

    assert(crimes.size == validRows.size)

    val firstCrime = crimes.head
    assert(firstCrime.crimeId.contains(id))
    assert(firstCrime.districtName.contains(districtName))
    assert(firstCrime.latitude.contains(latitude))
    assert(firstCrime.longitude.contains(longitude))
    assert(firstCrime.crimeType.contains(crimeType))
    assert(firstCrime.lastOutcome.contains(lastOutcome))
  }
}
