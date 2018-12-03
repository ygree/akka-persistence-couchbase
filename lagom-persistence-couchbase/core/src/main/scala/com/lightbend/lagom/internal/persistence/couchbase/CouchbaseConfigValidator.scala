/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.internal.persistence.couchbase

import akka.event.LoggingAdapter
import com.typesafe.config.Config

private[lagom] object CouchbaseConfigValidator {

  def validateBucket(namespace: String, config: Config, log: LoggingAdapter): Unit =
    if (log.isErrorEnabled) {
      val bucketPath = s"$namespace.bucket"
      if (!config.hasPath(bucketPath)) {
        log.error("Configuration for [{}] must be set in application.conf ", bucketPath)
      }
    }
}
