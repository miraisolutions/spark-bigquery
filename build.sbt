import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import com.typesafe.sbt.license.{DepModuleInfo, LicenseInfo}


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
  // TODO: use released version once available;
  // currently using (local) SNAPSHOT version with spark-avro 4.0.0
  "com.spotify" %% "spark-bigquery" % "0.2.2-SNAPSHOT" excludeAll(
    ExclusionRule("com.fasterxml.jackson.core", "jackson-core"), // clashes with Spark 2.2.x
    ExclusionRule("commons-logging", "commons-logging"), // clashes with Spark 2.2.x
    ExclusionRule("commons-lang", "commons-lang") // clashes with Spark 2.2.x
  )
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Shade google guava dependency due to version mismatch between bigquery connector and Spark
// See https://github.com/spotify/spark-bigquery/issues/12
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "shadegooglecommon.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _) =>
    MergeStrategy.discard
  case PathList("com", "databricks", "spark", "avro", xs @ _*) =>
    // NOTE: "com.spotify" %% "spark-bigquery" provides a modified implementation of
    // com.databricks.spark.avro.SchemaConverters
    MergeStrategy.first
  case _ =>
    MergeStrategy.singleOrError
}

// Exclude avro-ipc tests jar
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { _.data.getName == "avro-ipc-1.7.7-tests.jar" }
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

licenseConfigurations := Set("compile")

licenseOverrides := {
  case DepModuleInfo("org.slf4j", "slf4j-simple", _) =>
    LicenseInfo.MIT
}

val browser = JsoupBrowser()

def existsUrl(url: String): Boolean = {
  import java.net.{URL, HttpURLConnection}
  (new URL(url)).openConnection().asInstanceOf[HttpURLConnection].getResponseCode == 200
}

// Extends license report to include artifact description and link to JAR files
licenseReportNotes := {
  // TODO: remove this case once the released version is available
  case DepModuleInfo("com.spotify", "spark-bigquery_2.11", "0.2.2-SNAPSHOT") =>
    "Spark Bigquery" + '\u001F' + "spark-bigquery" + '\u001F' +
      "N/A (waiting for next released version integrating https://github.com/spotify/spark-bigquery/pull/47)" + '\u001F' + "N/A"
  case DepModuleInfo(group, id, version) =>
    try {
      // Fetch artifact information
      val doc = browser.get(s"https://mvnrepository.com/artifact/$group/$id/$version")
      // Extract title
      val title = (doc >> text(".im-title")).replaceFirst("\\s»\\s.+$", "")
      // Extract description
      val description = doc >> text(".im-description")
      // Locate link to JAR file
      val mainJar = (doc >> elementList("a.vbtn"))
        .filter(element => element.innerHtml.startsWith("jar") || element.innerHtml.startsWith("bundle"))
        .map(_ >> attr("href"))
        .headOption
        .getOrElse(throw new NoSuchElementException("Can't locate JAR file"))

      // Derive link to sources JAR file
      val sourcesJar = mainJar.replaceFirst("\\.jar$", "-sources.jar")

      // Check if JAR file exists
      require(existsUrl(mainJar), "Invalid link to JAR file")
      // Check if sources JAR file exists
      require(existsUrl(sourcesJar), "Invalid link to sources JAR file")
      // https://en.wikipedia.org/wiki/C0_and_C1_control_codes (unit separator)
      title + '\u001F' + description + '\u001F' + mainJar + '\u001F' + sourcesJar
    } catch {
      case t: Throwable =>
        "**** " + t.getMessage + " ****"
    }
}
