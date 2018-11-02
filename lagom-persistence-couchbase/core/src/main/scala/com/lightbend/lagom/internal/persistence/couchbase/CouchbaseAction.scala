/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.persistence.couchbase

import akka.Done
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession

import scala.concurrent.{ ExecutionContext, Future }

trait CouchbaseAction {
  def execute(ab: CouchbaseSession, ec: ExecutionContext): Future[Done]
}
