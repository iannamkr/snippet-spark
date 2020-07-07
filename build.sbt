name := "spark-snippet"

version := "0.1"



val sparkVersion = "3.0.0"
val scalatestVersion = "3.2.0"

scalaVersion := "2.12.11"
scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-language:higherKinds"
)
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.scalatest" %% "scalatest" % scalatestVersion % "test",
)

