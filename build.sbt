import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import com.typesafe.sbt.license.{DepModuleInfo, LicenseInfo}


name := "spark-bigquery"

organization := "com.miraisolutions"

organizationName := "Mirai Solutions GmbH"

version := "0.1.0-SNAPSHOT"

startYear := Some(2018)

licenses += ("MIT", new URL("https://opensource.org/licenses/MIT"))

scalaVersion := "2.11.12"

scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature",
  "-unchecked"
)

resolvers += Opts.resolver.sonatypeReleases

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "com.google.cloud" % "google-cloud-bigquery" % "1.34.0",
  "com.google.cloud.bigdataoss" % "gcs-connector" % "1.8.1-hadoop2",
  "com.databricks" %% "spark-avro" % "4.0.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "it,test",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_0.9.0" % "it,test",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "it,test" // required by spark-testing-base
)

excludeDependencies ++= Seq(
  ExclusionRule("com.fasterxml.jackson.core", "jackson-core"), // clashes with Spark 2.2.x
  ExclusionRule("commons-logging", "commons-logging"), // clashes with Spark 2.2.x
  ExclusionRule("commons-lang", "commons-lang") // clashes with Spark 2.2.x
)

configs(IntegrationTest)

Defaults.itSettings ++ headerSettings(IntegrationTest)

IntegrationTest / fork := true

IntegrationTest / javaOptions ++= Seq(
  "-Xmx2048m",
  "-Xms512m",
  "-XX:+CMSClassUnloadingEnabled"
)

IntegrationTest / logBuffered := false

enablePlugins(SbtProguard)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Shade google dependencies due to version mismatches with dependencies deployed on Google Dataproc
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadegoogle.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", _) =>
    MergeStrategy.discard

  case PathList("META-INF", "maven", _*) =>
    MergeStrategy.discard

  case _ =>
    MergeStrategy.singleOrError
}

// Exclude avro-ipc tests jar
assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp filter { _.data.getName == "avro-ipc-1.7.7-tests.jar" }
}

javaOptions in (Proguard, proguard) := Seq(
  "-Xmx4g"
)

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
    "-keep class shadegoogle.cloud.** { *; }",
    "-keep class shadegoogle.common.** { *; }",
    "-keep class shadegoogle.auth.** { *; }",
    "-keep class com.miraisolutions.spark.bigquery.** { *; }"
  )

proguardInputs in Proguard := Seq(baseDirectory.value / "target" / s"scala-${scalaVersion.value.dropRight(3)}" /
  s"${name.value}-assembly-${version.value}.jar")

proguardMerge in Proguard := false

licenseConfigurations := Set("compile")

licenseOverrides := {
  case DepModuleInfo("org.slf4j", "slf4j-api", _) =>
    LicenseInfo.MIT
}

val browser = JsoupBrowser()

def existsUrl(url: String): Boolean = {
  import java.net.{URL, HttpURLConnection}
  (new URL(url)).openConnection().asInstanceOf[HttpURLConnection].getResponseCode == 200
}

// Extends license report to include artifact description and link to JAR files
licenseReportNotes := {
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
