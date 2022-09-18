name := "TestExample"

version := "0.1"

scalaVersion := "2.12.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.scalactic" %% "scalactic" % "3.0.8" % Test,
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)