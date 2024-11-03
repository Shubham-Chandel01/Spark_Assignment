ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .settings(
    name := "Spark_Assignment"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.scalatest" %% "scalatest" % "3.2.18" % Test
)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.10" % Test