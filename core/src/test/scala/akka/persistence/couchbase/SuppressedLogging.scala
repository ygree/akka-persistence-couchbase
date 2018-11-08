/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase
import akka.actor.ActorSystem
import akka.event.Logging
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Decrease log level to avoid "exceeded the maximum log length" when run by Travis
 */
trait SuppressedLogging extends BeforeAndAfterAll { this: Suite =>

  def system: ActorSystem

  override protected def beforeAll(): Unit = {
    system.eventStream.setLogLevel(Logging.InfoLevel)
    super.beforeAll()
  }
}
