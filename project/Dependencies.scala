/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */

import sbt._

object Dependencies {

  val AkkaVersion = "2.5.15"

  object Compile {
    val couchbaseClient =       "com.couchbase.client"  % "java-client"             % "2.6.0"  // Apache V2
    val couchbaseDcpClient =    "com.couchbase.client"  % "dcp-client"              % "0.19.0" // Apache V2

    // used to easily convert rxjava into reactive streams and then into akka streams
    val rxJavaReactiveStreams = "io.reactivex"          % "rxjava-reactive-streams" % "1.2.1" // Apache V2
    val akkaPersistence =       "com.typesafe.akka"    %% "akka-persistence"        % AkkaVersion
    val akkaPersistenceQuery =  "com.typesafe.akka"    %% "akka-persistence-query"  % AkkaVersion
  }

  object Test {
    val akkaPersistenceTck = "com.typesafe.akka"       %% "akka-persistence-tck"    % AkkaVersion % "test"
    val akkaStreamTestkit =  "com.typesafe.akka"       %% "akka-stream-testkit"     % AkkaVersion % "test"
    val logback =            "ch.qos.logback"           % "logback-classic"         % "1.2.3"     % "test" // EPL 1.0 / LGPL 2.1
    val scalaTest =          "org.scalatest"           %% "scalatest"               % "3.0.4"     % "test" // ApacheV2
  }

  import Compile._
  import Test._

  val core = Seq(
    couchbaseClient, couchbaseDcpClient, rxJavaReactiveStreams, akkaPersistence, akkaPersistenceQuery,
    akkaPersistenceTck, akkaStreamTestkit, logback, scalaTest
  )

}
