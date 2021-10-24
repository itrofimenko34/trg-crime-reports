package trg.model.kpi

/**
 *
 */
sealed trait KPIType {
  val name: String
  override def toString: String = name
}

/**
 *
 */
object KPIType {
  def fromString(str: String): KPIType =
    str match {
      case Count.name => Count
      case Percent.name => Percent
      case _ => throw new Exception(s"KPI Type '$str' is not supported")
    }
}

/**
 *
 */
case object Count extends KPIType {
  val name = "count"
}

/**
 *
 */
case object Percent extends KPIType {
  val name = "percent"
}

