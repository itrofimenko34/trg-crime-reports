package trg.model.kpi

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{asc, desc, col}

/**
 * Represents a model for KPIProcessor input parameters
 * @param inputPath - path to the input data
 * @param limit - number of rows in result. default - 20
 * @param ordering - sort ordering . default - desc
 * @param kpiType - the type of kpi to calculate (count/percent)
 * @param groupKeys - a comma-separated column names set to use as a group keys
 */
case class KPIParameters(
                            inputPath: String,
                            limit: Int, ordering:
                            String => Column,
                            kpiType: KPIType,
                            groupKeys: Option[Array[Column]]
                          )
object KPIParameters {

  def apply(argsMap: Map[String, String]): KPIParameters = {
    val inputPath: String = argsMap
      .getOrElse("inputPath", throw new Exception("Required argument 'inputPath' is not specified!"))

    val limit: Int = argsMap.get("limit").map(_.toInt).getOrElse(20)

    val ordering: String => Column = argsMap.get("ordering") match {
      case Some("asc") => asc
      case _ => desc
    }

    val kpiType: KPIType = KPIType.fromString(
      argsMap.getOrElse("kpiType", throw new Exception("Required argument 'kpi' is not specified!"))
    )

    val groupKeys: Option[Array[Column]] = argsMap
      .get("groupKeys")
      .map(_.split(","))
      .map(_.map(col))

    KPIParameters(inputPath, limit, ordering, kpiType, groupKeys)
  }
}

