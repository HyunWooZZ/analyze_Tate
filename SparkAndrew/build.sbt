ThisBuild / version := "1.7.2"
ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "SparkAndrew",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.2",
      "org.apache.spark" %% "spark-sql" % "3.2.2",
      "org.apache.spark" %% "spark-streaming" % "3.2.2"
    )
  )
