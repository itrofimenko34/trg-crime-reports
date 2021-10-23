name := "trg-crime-reports"

version := "0.1"

scalaVersion := "2.12.10"

crossPaths := false

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.1.1",
  "org.scalatest" %% "scalatest" % "3.2.10" % Test
)
