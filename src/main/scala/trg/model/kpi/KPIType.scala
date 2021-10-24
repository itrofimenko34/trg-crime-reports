package trg.model.kpi

/**
 * Enumerator for KPI types
 */
sealed trait KPIType {
  val name: String
  override def toString: String = name
}

object KPIType {
  def fromString(str: String): KPIType =
    str match {
      case Count.name => Count
      case Percent.name => Percent
      case _ => throw new Exception(s"KPI Type '$str' is not supported")
    }
}

/**
 *  Count KPI enum
 */
case object Count extends KPIType {
  val name = "count"
}

/**
 * Percent KPI enum
 */
case object Percent extends KPIType {
  val name = "percent"
}

