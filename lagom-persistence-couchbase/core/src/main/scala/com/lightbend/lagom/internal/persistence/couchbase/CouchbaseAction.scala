/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.persistence.couchbase

import akka.Done
import com.couchbase.client.java.AsyncBucket

import scala.concurrent.{ ExecutionContext, Future }

trait CouchbaseAction {
  def execute(ab: AsyncBucket, ec: ExecutionContext): Future[Done]
}
