name := "trg-crime-reports"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
