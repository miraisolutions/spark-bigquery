name := "spark-bigquery"

organization := "com.miraisolutions"

version := "0.2-SNAPSHOT"

scalaVersion := "2.11.11"

resolvers += Opts.resolver.sonatypeReleases

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
  "org.jgrapht" % "jgrapht-core" % "1.0.1",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  // Exclude jackson dependency due to version mismatch between bigquery connector and Spark
  "com.spotify" %% "spark-bigquery" % "0.2.1" exclude ("com.fasterxml.jackson.core", "jackson-core")
)

// Shade google guava dependency due to version mismatch between bigquery connector and Spark
// See https://github.com/spotify/spark-bigquery/issues/12
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

retrieveManaged := true
retrievePattern := "[organisation]-[module]-[artifact](-[revision])(-[classifier]).[ext]"
