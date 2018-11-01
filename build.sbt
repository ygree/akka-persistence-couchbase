import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import sbt.Keys.{name, publishArtifact}

crossScalaVersions := Seq("2.12.6", "2.11.12")

def common: Seq[Setting[_]] = scalariformSettings(true) ++ Seq(
  organization := "com.lightbend.akka",
  organizationName := "Lightbend Inc.",
  startYear := Some(2018),
  licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
  crossScalaVersions := Seq("2.11.12", "2.12.7"),
  scalaVersion := crossScalaVersions.value.last,
  crossVersion := CrossVersion.binary,

  scalacOptions ++= Seq(
    "-encoding", "UTF-8",
    "-feature",
    "-unchecked",
    "-deprecation",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Xfuture"
  ),

  headerLicense := Some(HeaderLicense.Custom(
    """Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>"""
  )),

  logBuffered in Test := System.getProperty("akka.logBufferedTests", "false").toBoolean,

  // show full stack traces and test case durations
  testOptions in Test += Tests.Argument("-oDF"),

  // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
  // -a Show stack traces and exception class name for AssertionErrors.
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),

  // disable parallel tests
  parallelExecution in Test := false,

  ScalariformKeys.preferences in Compile  := formattingPreferences,
  ScalariformKeys.preferences in Test     := formattingPreferences
)

lazy val root = (project in file("."))
  .settings(common)
  .settings(
    name := "akka-persistence-couchbase-root",
    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
    publish := {}
  )
  .aggregate((lagomModules ++ Seq(core)).map(Project.projectToRef): _*)

lazy val core = (project in file("core"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(common)
  .settings(
    name := "akka-persistence-couchbase",
    libraryDependencies := Dependencies.core
  )

lazy val lagomModules = Seq[Project](
  `lagom-persistence-couchbase-core`,
  `lagom-persistence-couchbase-javadsl`,
  `lagom-persistence-couchbase-scaladsl`
)

lazy val `lagom-persistence-couchbase-core` = (project in file("lagom-persistence-couchbase/core"))
  .dependsOn(core % "compile;test->test")
  .settings(common)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "lagom-persistence-couchbase-core",
    libraryDependencies := Dependencies.`lagom-persistence-couchbase-core`
  )

lazy val `lagom-persistence-couchbase-javadsl` = (project in file("lagom-persistence-couchbase/javadsl"))
  .dependsOn(
    core % "compile;test->test",
    `lagom-persistence-couchbase-core` % "compile;test->test"
  )
  .settings(common)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "lagom-javadsl-persistence-couchbase",
    libraryDependencies := Dependencies.`lagom-persistence-couchbase-javadsl`
  )

lazy val `lagom-persistence-couchbase-scaladsl` = (project in file("lagom-persistence-couchbase/scaladsl"))
  .dependsOn(
    core % "compile;test->test",
    `lagom-persistence-couchbase-core` % "compile;test->test"
  )
  .settings(common)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "lagom-scaladsl-persistence-couchbase",
    libraryDependencies := Dependencies.`lagom-persistence-couchbase-scaladsl`
  )

def formattingPreferences = {
  import scalariform.formatter.preferences._
  FormattingPreferences()
    .setPreference(RewriteArrowSymbols, false)
    .setPreference(AlignParameters, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(SpacesAroundMultiImports, true)
}