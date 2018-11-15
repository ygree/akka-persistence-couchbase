import sbt.Keys.{name, publishArtifact}

crossScalaVersions := Seq("2.12.6", "2.11.12")

def common: Seq[Setting[_]] = Seq(
  organization := "com.lightbend.akka",
  organizationName := "Lightbend Inc.",
  startYear := Some(2018),
  licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
  crossScalaVersions := Seq("2.11.12", "2.12.7"),
  scalaVersion := crossScalaVersions.value.last,
  crossVersion := CrossVersion.binary,
  scalafmtOnCompile := true,
  scalacOptions ++= Seq(
    "-encoding",
    "UTF-8",
    "-feature",
    "-unchecked",
    "-deprecation",
    "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Xfuture"
  ),
  // Setting javac options in common allows IntelliJ IDEA to import them automatically
  javacOptions in compile ++= Seq(
    "-encoding",
    "UTF-8",
    "-source",
    "1.8",
    "-target",
    "1.8",
    "-parameters", // This param is required for Jackson serialization to preserve method parameter names
    "-Xlint:unchecked",
    "-Xlint:deprecation"
  ),
  headerLicense := Some(
    HeaderLicense.Custom(
      """Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>"""
    )
  ),
  logBuffered in Test := System.getProperty("akka.logBufferedTests", "false").toBoolean,
  // show full stack traces and test case durations
  testOptions in Test += Tests.Argument("-oDF"),
  // -v Log "test run started" / "test started" / "test run finished" events on log level "info" instead of "debug".
  // -a Show stack traces and exception class name for AssertionErrors.
  testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
  // disable parallel tests
  parallelExecution in Test := false
)

lazy val root = (project in file("."))
  .settings(common)
  .settings(
    name := "akka-persistence-couchbase-root",
    publishArtifact := false,
    publishTo := Some(Resolver.file("Unused transient repository", file("target/unusedrepo"))),
    publish := {}
  )
  .aggregate((Seq(couchbaseClient, core) ++ lagomModules).map(Project.projectToRef): _*)

// TODO this should eventually be an alpakka module
lazy val couchbaseClient = (project in file("couchbase-client"))
  .settings(common)
  .settings(
    name := "akka-persistence-couchbase-client",
    libraryDependencies := Dependencies.couchbaseClient
  )

lazy val core = (project in file("core"))
  .enablePlugins(AutomateHeaderPlugin)
  .settings(common)
  .settings(
    name := "akka-persistence-couchbase",
    libraryDependencies := Dependencies.core
  )
  .dependsOn(couchbaseClient)

lazy val lagomModules = Seq[Project](
  `lagom-persistence-couchbase-core`,
  `lagom-persistence-couchbase-javadsl`,
  `lagom-persistence-couchbase-scaladsl`
)

/**
 * This module contains copy-pasted parts from Lagom project that are not available outside of the project
 * because they are not published as part of the result artifacts.
 *
 * This module combines the reusable parts that reside in Lagom project in next modules:
 *
 * persistence/core
 * persistence/javadsl
 * persistence/scaladsl
 *
 * For simplicity sake here they are combined into one module.
 *
 * TODO: It can be removed once it's resolved (see https://github.com/lagom/lagom/issues/1634)
 */
lazy val `copy-of-lagom-persistence-test` =
  (project in file("lagom-persistence-couchbase/copy-of-lagom-persistence-test"))
    .settings(common)
    .settings(
      // This modules copy-pasted preserve it as is
      scalafmtOnCompile := false,
      publishArtifact := false,
      libraryDependencies := Dependencies.`copy-of-lagom-persistence-test`
    )

lazy val `lagom-persistence-couchbase-core` = (project in file("lagom-persistence-couchbase/core"))
  .dependsOn(core % "compile;test->test", couchbaseClient)
  .settings(common)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "lagom-persistence-couchbase-core",
    libraryDependencies := Dependencies.`lagom-persistence-couchbase-core`
  )

lazy val `lagom-persistence-couchbase-javadsl` = (project in file("lagom-persistence-couchbase/javadsl"))
  .dependsOn(
    core % "compile;test->test",
    `lagom-persistence-couchbase-core` % "compile;test->test",
    `copy-of-lagom-persistence-test` % "test->test"
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
    `lagom-persistence-couchbase-core` % "compile;test->test",
    `copy-of-lagom-persistence-test` % "test->test"
  )
  .settings(common)
  .enablePlugins(AutomateHeaderPlugin)
  .settings(
    name := "lagom-scaladsl-persistence-couchbase",
    libraryDependencies := Dependencies.`lagom-persistence-couchbase-scaladsl`
  )
