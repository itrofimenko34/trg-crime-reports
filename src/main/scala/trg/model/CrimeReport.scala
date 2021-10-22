package trg.model

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class CrimeReport(
                        crimeId: Option[String],
                        districtName: String,
                        latitude: Option[Double],
                        longitude: Option[Double],
                        crimeType: Option[String],
                        lastOutcome: Option[String]
                      )

object CrimeReport {
  val joinKey = "crimeId"

  val crimeIdLabel = "Crime ID"
  val districtNameLabel = "districtName"
  val latitudeLabel = "Latitude"
  val longitudeLabel = "Longitude"
  val crimeTypeLabel = "Crime type"
  val lastOutcomeLabel = "Last outcome category"

  val validationSchema: StructType = StructType(Seq(
    StructField(crimeIdLabel, StringType, nullable = true),
    StructField(districtNameLabel, StringType, nullable = true),
    StructField(latitudeLabel, StringType, nullable = true),
    StructField(longitudeLabel, StringType, nullable = true),
    StructField(crimeTypeLabel, StringType, nullable = true),
    StructField(lastOutcomeLabel, StringType, nullable = true)
  ))

  def apply(row: Row): CrimeReport = {
    val crimeId = Option(row.getAs[String](crimeIdLabel))
    val districtName = row.getAs[String](districtNameLabel)
    val latitude = Option(row.getAs[String](latitudeLabel)).map(_.toDouble)
    val longitude = Option(row.getAs[String](longitudeLabel)).map(_.toDouble)
    val crimeType = Option(row.getAs[String](crimeTypeLabel))
    val lastOutcome = Option(row.getAs[String](lastOutcomeLabel))

    new CrimeReport(crimeId, districtName, latitude, longitude, crimeType, lastOutcome)
  }
}
