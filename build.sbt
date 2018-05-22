
scalaVersion := "2.11.12"

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
      "org.apache.spark" %% "spark-sql" % sparkVersion
    )
  )

