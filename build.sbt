name := "spark-bigquery"

organization := "com.miraisolutions"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.11"

resolvers += Opts.resolver.sonatypeReleases

enablePlugins(SbtProguard)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  // Exclude jackson dependency due to version mismatch between bigquery connector and Spark
  "com.spotify" %% "spark-bigquery" % "0.2.2-SNAPSHOT" excludeAll(
    ExclusionRule("com.fasterxml.jackson.core", "jackson-core"), // clashes with Spark 2.2.x
    ExclusionRule("commons-logging", "commons-logging") // clashes with Spark 2.2.x
  ) //,
  // "com.databricks" %% "spark-avro" % "4.0.0" // need newer Spark 2.2.x compatible version
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Shade google guava dependency due to version mismatch between bigquery connector and Spark
// See https://github.com/spotify/spark-bigquery/issues/12
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "shadegooglecommon.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _) => MergeStrategy.discard
  case PathList("com", "databricks", "spark", "avro", xs @ _*) => MergeStrategy.first
  case _ => MergeStrategy.singleOrError
}

// https://github.com/sbt/sbt-proguard/issues/23
// https://stackoverflow.com/questions/39655207/how-to-obfuscate-fat-scala-jar-with-proguard-and-sbt

proguardOptions in Proguard ++=
  Seq(
    "-dontobfuscate",
    "-dontoptimize",
    "-dontnote",
    "-dontwarn",
    "-ignorewarnings",
    // "-dontskipnonpubliclibraryclasses",
    // "-dontskipnonpubliclibraryclassmembers",
    // "-keepparameternames",
    // "-keepattributes *",
    // https://stackoverflow.com/questions/33189249/how-to-tell-proguard-to-keep-enum-constants-and-fields
    """-keepclassmembers class * extends java.lang.Enum {
      |    <fields>;
      |    public static **[] values();
      |    public static ** valueOf(java.lang.String);
      |}""".stripMargin,
    "-keep class org.apache.avro.** { *; }",
    "-keep class com.databricks.spark.avro.** { *; }",
    "-keep class com.google.cloud.hadoop.** { *; }",
    "-keep class com.spotify.spark.bigquery.** { *; }",
    "-keep class com.miraisolutions.spark.bigquery.DefaultSource { *; }"
  )

proguardInputs in Proguard := Seq(baseDirectory.value / "target" / s"scala-${scalaVersion.value.dropRight(3)}" / s"${name.value}-assembly-${version.value}.jar")

proguardMerge in Proguard := false
