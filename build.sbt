val AkkaVersion = "2.5.15"

val dependencies = Seq(
  "com.couchbase.client" % "java-client" % "2.6.0",
  // used to easily convert rxjava into reactive streams and then into akka streams
  "io.reactivex" % "rxjava-reactive-streams" % "1.2.1",
  "com.typesafe.akka" %% "akka-persistence" % AkkaVersion,
  "com.typesafe.akka" %% "akka-persistence-query" % AkkaVersion,
  "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % "test",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test"
)

lazy val root = (project in file("."))
  .settings(
    name := "akka-persistence-couchbase",
    libraryDependencies ++= dependencies
  )
