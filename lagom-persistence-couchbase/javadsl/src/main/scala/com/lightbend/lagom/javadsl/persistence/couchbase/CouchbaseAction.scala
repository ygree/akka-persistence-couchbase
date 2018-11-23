/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase

import java.util.concurrent.CompletionStage

import akka.Done
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession

trait CouchbaseAction {
  def execute(cs: CouchbaseSession): CompletionStage[Done]
}
