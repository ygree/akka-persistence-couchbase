/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.persistence.couchbase

import akka.actor.ActorSystem
import akka.event.Logging
import akka.testkit.EventFilter
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, MustMatchers, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.Await

class MyException extends RuntimeException("MyException")

class CouchbaseConfigValidatorSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {

  val akkaTestLogging = ConfigFactory.parseString("akka.loggers = [akka.testkit.TestEventListener]")
  implicit val system = ActorSystem("test", akkaTestLogging)
  val log = Logging(system, classOf[CouchbaseConfigValidatorSpec])

  override def afterAll =
    Await.result(system.terminate(), Duration.Inf)

  "CouchbaseConfigValidator" should {
    "detect when bucket is not set" in {
      val config = ConfigFactory.parseString("""some.config.setting = 1""".stripMargin)
      EventFilter
        .error("Configuration for [test.bucket] must be set in application.conf ", occurrences = 1)
        .intercept {
          CouchbaseConfigValidator.validateBucket("test", config, log)
        }
    }
    "detect when bucket is set to null" in {
      val config = ConfigFactory.parseString("""testpath1.bucket = null""".stripMargin)
      EventFilter
        .error("Configuration for [testpath1.bucket] must be set in application.conf ", occurrences = 1)
        .intercept {
          CouchbaseConfigValidator.validateBucket("testpath1", config, log)
        }
    }
    "pass when bucket is specified" in {
      val config = ConfigFactory.parseString("""sample.path.bucket = bucketname""".stripMargin)
      // expect only one "another error" in the log
      EventFilter.error(occurrences = 1) intercept {
        CouchbaseConfigValidator.validateBucket("sample.path", config, log)
        log.error("another error")
      }
    }
  }
}
