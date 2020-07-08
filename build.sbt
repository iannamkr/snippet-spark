name := "spark-snippet"

version := "0.1"

val sparkVersion = "3.0.0"
val scalatestVersion = "3.2.0"
val scaldingVersion = "0.17.4"
val algebirdVersion = "0.13.7"

scalaVersion := "2.12.11"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-language:higherKinds"
)

resolvers += "Cascading libraries" at "https://conjars.org/repo"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "com.twitter" %% "scalding-core" % scaldingVersion,
    "com.twitter" %% "algebird-spark" % algebirdVersion,
    "org.scalatest" %% "scalatest" % scalatestVersion % "test",
)

