package trg.model

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class CrimeOutcome(crimeId: Option[String], outcomeType: Option[String])

object CrimeOutcome {
  val joinKey = "crimeId"

  val crimeIdLabel = "Crime ID"
  val outcomeTypeLabel = "Outcome type"

  val validationSchema: StructType = StructType(Seq(
    StructField(crimeIdLabel, StringType, nullable = true),
    StructField(outcomeTypeLabel, StringType, nullable = true)
  ))

  def apply(row: Row): CrimeOutcome = {
    val crimeId = Option(row.getAs[String](crimeIdLabel))
    val outcomeType = Option(row.getAs[String](outcomeTypeLabel))

    new CrimeOutcome(crimeId, outcomeType)
  }
}


