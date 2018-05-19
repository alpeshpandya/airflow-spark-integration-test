
scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.alpesh.integration",
      scalaVersion := "2.12.4",
      version      := "0.1.0"
    )),

    name := "rides_by_humidity",
    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.0.5",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "com.amazonaws" % "aws-java-sdk" % "1.10.47" exclude("com.fasterxml.jackson.core", "jackson-databind"),
      "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.7",
      "com.typesafe" % "config" % "1.3.2",
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3"
    )
  )

