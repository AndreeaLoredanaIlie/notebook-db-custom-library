organization := "db.custom.lib" //GroupID
name := "dbOps" //Artifact ID

version := "0.7"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided",
  "com.databricks" %% "dbutils-api" % "0.0.4" % "provided",
  "com.microsoft.azure" % "azure-sqldb-spark" % "1.0.2")


