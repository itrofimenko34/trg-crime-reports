package trg.model

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.scalatest.funsuite.AnyFunSuite

class CrimeOutcomeTest extends AnyFunSuite {

  test("CrimeOutcome should properly convert valid rows") {
    val id = "id1"
    val outcomeType = "outcomeType1"

    val validData: List[Array[Any]] = List(
      Array(id, outcomeType),
      Array(id, null)
    )

    val validRows = validData.map(arr => new GenericRowWithSchema(arr, CrimeOutcome.validationSchema))

    val outcomes = validRows.map(CrimeOutcome.apply)

    assert(outcomes.size == validRows.size)

    val firstOutcome = outcomes.head
    assert(firstOutcome.crimeId.contains(id))
    assert(firstOutcome.outcomeType.contains(outcomeType))
  }
}
