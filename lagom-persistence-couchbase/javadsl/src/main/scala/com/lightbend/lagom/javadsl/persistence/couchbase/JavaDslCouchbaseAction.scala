/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseAction
import scala.compat.java8.FutureConverters._

trait JavaDslCouchbaseAction {
  def execute(ab: CouchbaseSession): CompletionStage[Done]
}

object JavaDslCouchbaseAction {
  import scala.concurrent.ExecutionContext
  def apply(action: CouchbaseAction)(implicit ec: ExecutionContext): JavaDslCouchbaseAction =
    new JavaDslCouchbaseAction {
      override def execute(ab: CouchbaseSession): CompletionStage[Done] =
        action.execute(ab.asScala, ec).toJava
    }
}
