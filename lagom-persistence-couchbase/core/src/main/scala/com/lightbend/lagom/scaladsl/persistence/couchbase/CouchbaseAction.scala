/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase

import akka.Done
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession

import scala.concurrent.{ExecutionContext, Future}

trait CouchbaseAction {
  def execute(cs: CouchbaseSession, ec: ExecutionContext): Future[Done]
}
