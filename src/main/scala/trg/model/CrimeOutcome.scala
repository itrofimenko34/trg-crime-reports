package trg.model

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Represents a single crime outcome
 * @param crimeId - unique id of crime
 * @param outcomeType - the outcome of crime
 */
case class CrimeOutcome(crimeId: Option[String], outcomeType: Option[String])

object CrimeOutcome {
  val joinKey = "crimeId"

  val crimeIdLabel = "Crime ID"
  val outcomeTypeLabel = "Outcome type"

  /**
   * Schema of input data that can be converted into CrimeOutcome
   */
  val validationSchema: StructType = StructType(Seq(
    StructField(crimeIdLabel, StringType, nullable = true),
    StructField(outcomeTypeLabel, StringType, nullable = true)
  ))

  /**
   * Converts spark row into CrimeOutcome instance
   * @param row - input csv row that satisfy <validationSchema>
   * @return CrimeOutcome instance
   */
  def apply(row: Row): CrimeOutcome = {
    val crimeId = Option(row.getAs[String](crimeIdLabel))
    val outcomeType = Option(row.getAs[String](outcomeTypeLabel))

    new CrimeOutcome(crimeId, outcomeType)
  }
}


