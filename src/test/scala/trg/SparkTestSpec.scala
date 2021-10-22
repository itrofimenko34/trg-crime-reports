package trg

import org.apache.spark.sql.SparkSession

trait SparkTestSpec {
  val spark: SparkSession = SparkSession.builder().master("local[1]").getOrCreate()
}