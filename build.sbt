import net.ruippeixotog.scalascraper.browser.JsoupBrowser
import net.ruippeixotog.scalascraper.dsl.DSL._
import net.ruippeixotog.scalascraper.dsl.DSL.Extract._
import com.typesafe.sbt.license.{DepModuleInfo, LicenseInfo}
import ReleaseTransformations._
import sbtassembly.MappingSet
import sbtrelease.{Version, versionFormatError}

import scala.xml.{Elem, Node => XmlNode, NodeSeq => XmlNodeSeq}
import scala.xml.transform._

// Apache Spark version setting
val sparkVersion = settingKey[String]("The version of Spark to use.")

// Custom task for creating a Spark package release artifact
val sparkPackage = taskKey[File]("Creates a Spark package release artifact.")

// Setting Maven properties as needed by gcs-connector
val mavenProps = settingKey[Unit]("Setting Maven properties")

lazy val commonSettings = Seq(
  // Name must match github repository name
  name := "spark-bigquery",
  organization := "com.miraisolutions",
  organizationName := "Mirai Solutions GmbH",
  description := "A Google BigQuery Data Source for Apache Spark",
  startYear := Some(2018),
  licenses += ("MIT", new URL("https://opensource.org/licenses/MIT")),
  sparkVersion := "2.3.0",
  scalaVersion := "2.11.12",
  crossScalaVersions := Seq("2.11.12"),
  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked"
  )
)

// Dependency exclusions
lazy val exclusions = Seq(
  // Clash with Spark
  ExclusionRule("com.fasterxml.jackson.core", "jackson-core"),
  ExclusionRule("commons-logging", "commons-logging"),
  ExclusionRule("commons-lang", "commons-lang"),
  // Not required
  ExclusionRule("com.google.auto.value", "auto-value"),
  ExclusionRule("com.google.auto.value", "auto-value-annotations")
)

// Spark provided dependencies
lazy val sparkDependencies = Def.setting(Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion.value % "provided"
))

// Dependencies which need to be shaded to run on Google Cloud Dataproc
lazy val dependenciesToShade = Seq(
  "com.google.cloud" % "google-cloud-bigquery" % "1.37.1" excludeAll(exclusions: _*),
  "com.google.cloud.bigdataoss" % "gcs-connector" % "1.9.3-hadoop2" excludeAll(exclusions: _*)
)

// Dependencies which don't need any shading
lazy val nonShadedDependencies = Seq(
  "com.databricks" %% "spark-avro" % "4.0.0"
)

// Test dependencies
lazy val testDependencies = Def.setting(Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % "it,test",
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion.value}_0.9.0" % "it,test",
  "org.apache.spark" %% "spark-hive" % sparkVersion.value % "it,test" // required by spark-testing-base
))

lazy val browser = JsoupBrowser()

def existsUrl(url: String): Boolean = {
  import java.net.{URL, HttpURLConnection}
  (new URL(url)).openConnection().asInstanceOf[HttpURLConnection].getResponseCode == 200
}


lazy val root = (project in file("."))
  .enablePlugins(AssemblyPlugin, AutomateHeaderPlugin, TutPlugin)
  .configs(IntegrationTest)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= dependenciesToShade ++ sparkDependencies.value ++
      nonShadedDependencies.map(_ % "provided") ++ testDependencies.value ++
      sparkDependencies.value.map(_.withConfigurations(Some("tut"))),
    skip in publish := true,
    mavenProps := {
      // Required by gcs-connector
      sys.props("hadoop.identifier") = "hadoop2"
      ()
    },

    Defaults.itSettings,
    IntegrationTest / fork := true,
    IntegrationTest / javaOptions ++= Seq(
      "-Xmx2048m",
      "-Xms512m",
      "-XX:+CMSClassUnloadingEnabled"
    ),
    IntegrationTest / logBuffered := false,
    IntegrationTest / testOptions += Tests.Argument("-oF"),
    automateHeaderSettings(IntegrationTest),

    // See https://spark-packages.org/artifact-help
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    // Shade google dependencies due to version mismatches with dependencies deployed on Google Dataproc
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("com.google.**" -> "shadegoogle.@1").inAll
    ),
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") =>
        // Take our "shaded" version
        MergeStrategy.last // see also assembledMappings below
      case PathList("META-INF", "services", "shaded.org.apache.hadoop.fs.FileSystem") =>
        MergeStrategy.discard
      case r =>
        MergeStrategy.defaultMergeStrategy(r)
    },
    assembledMappings in assembly := (assembledMappings in assembly).value :+
      MappingSet(
        None,
        Vector(file("src/main/resources/META-INF/services/shaded.org.apache.hadoop.fs.FileSystem") ->
          "META-INF/services/org.apache.hadoop.fs.FileSystem")
      ),

    // We release the distribution module only
    releaseProcess := Seq.empty,

    licenseConfigurations := Set("compile"),
    licenseOverrides := {
      case DepModuleInfo("org.slf4j", "slf4j-api", _) =>
        LicenseInfo.MIT
    },
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
  )

// A "virtual" project with configurations to build Spark packages
// See https://spark-packages.org/artifact-help
lazy val distribution = (project in file("distribution"))
  .settings(commonSettings: _*)
  .settings(
    // Include the Scala binary version here
    version := s"${(root / version).value}-s_${scalaBinaryVersion.value}",
    libraryDependencies := nonShadedDependencies,
    // Spark packages need the github organization name as the group ID
    organization := "miraisolutions",
    crossPaths := false,
    pomExtra := {
      <url>https://github.com/miraisolutions/spark-bigquery</url>
      <scm>
        <url>git@github.com:miraisolutions/spark-bigquery.git</url>
        <connection>scm:git:git@github.com:miraisolutions/spark-bigquery.git</connection>
      </scm>
      <developers>
        <developer>
          <id>martinstuder</id>
          <name>Martin Studer</name>
          <url>https://github.com/martinstuder</url>
        </developer>
        <developer>
          <id>lambiase</id>
          <name>Nicola Lambiase</name>
          <url>https://github.com/lambiase</url>
        </developer>
      </developers>
    },
    // Spark packages need the github repository name as the artifact ID
    pomPostProcess := { (node: XmlNode) =>
      val rule = new RewriteRule {
        override def transform(n: XmlNode): XmlNodeSeq = n match {
          case n: Elem if n.label == "project" =>
            val updatedChildren = n.child map {
              case c if c.label == "artifactId" =>
                <artifactId>{normalizedName.value}</artifactId>

              case c =>
                c
            }
            n.copy(child = updatedChildren)

          case n =>
            n
        }
      }
      new RuleTransformer(rule)(node)
    },
    Compile / packageBin := (root / Compile / assembly).value,
    sparkPackage := {
      val jar = (Compile / packageBin).value
      val pom = makePom.value
      val packageName = s"${normalizedName.value}-${version.value}"
      val zipFile = target.value / s"$packageName.zip"
      IO.delete(zipFile)
      IO.zip(Seq(jar -> s"$packageName.jar", pom -> s"$packageName.pom"), zipFile)
      println(s"\nSpark Package created at: $zipFile\n")
      zipFile
    },

    releaseVersion := { _ =>
      Version((root / version).value).map(_.withoutQualifier.string).getOrElse(versionFormatError)
    },
    releaseVersionFile := (ThisBuild / baseDirectory).value / "version.sbt",
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      releaseStepTask(sparkPackage),
      setNextVersion,
      commitNextVersion,
      pushChanges
    )
  )
