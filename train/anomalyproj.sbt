name := "Anomaly Trainer"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

resolvers += Resolver.mavenLocal

/*
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

resolvers ++= Seq(
  "Sonatype Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype Releases" at "http://oss.sonatype.org/content/repositories/releases"
)
*/

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
)

libraryDependencies ++= Seq(
  "ml.dmlc" % "xgboost4j" % "0.8-SNAPSHOT",
  "ml.dmlc" % "xgboost4j-spark" % "0.8-SNAPSHOT"
)
