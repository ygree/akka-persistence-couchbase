/**
 * Copyright (C) 2009-2018 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase

import java.util.Optional

import akka.annotation.InternalApi
import com.couchbase.client.java.{ PersistTo, ReplicateTo }

import scala.util.{ Failure, Success, Try }
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

final class CouchbaseWriteSettings private (
  val parallelism: Int,
  val replicateTo: ReplicateTo,
  val persistTo:   PersistTo,
  val timeout:     Long,
  val timeUnit:    java.util.concurrent.TimeUnit) {

  def withParallelism(parallelism: Int): CouchbaseWriteSettings = copy(parallelism = parallelism)

  def withReplicateTo(replicateTo: ReplicateTo): CouchbaseWriteSettings = copy(replicateTo = replicateTo)

  def withPersistTo(persistTo: PersistTo): CouchbaseWriteSettings = copy(persistTo = persistTo)

  def withTimeOut(timeout: Long, timeUnit: java.util.concurrent.TimeUnit): CouchbaseWriteSettings =
    copy(timeout = timeout, timeUnit = timeUnit)

  private[this] def copy(
    parallelism: Int                           = parallelism,
    replicateTo: ReplicateTo                   = replicateTo,
    persistTo:   PersistTo                     = persistTo,
    timeout:     Long                          = timeout,
    timeUnit:    java.util.concurrent.TimeUnit = timeUnit) =
    new CouchbaseWriteSettings(parallelism, replicateTo, persistTo, timeout, timeUnit)

  override def equals(other: Any): Boolean = other match {
    case that: CouchbaseWriteSettings =>
      parallelism == that.parallelism &&
        replicateTo == that.replicateTo &&
        persistTo == that.persistTo &&
        timeout == that.timeout &&
        timeUnit == that.timeUnit
    case _ => false
  }

  override def hashCode(): Int = {
    31 * parallelism.hashCode() + 31 * replicateTo.hashCode() + 31 * persistTo.hashCode()
    +31 * timeout.hashCode() + 31 * timeUnit.hashCode()
  }
}

object CouchbaseWriteSettings {

  val default = apply()

  def apply(
    parallelism: Int                           = 1,
    replicateTo: ReplicateTo                   = ReplicateTo.ONE,
    persistTo:   PersistTo                     = PersistTo.NONE,
    timeout:     Long                          = 2L,
    timeUnit:    java.util.concurrent.TimeUnit = java.util.concurrent.TimeUnit.SECONDS): CouchbaseWriteSettings =
    new CouchbaseWriteSettings(parallelism, replicateTo, persistTo, timeout, timeUnit)

  def create(): CouchbaseWriteSettings = CouchbaseWriteSettings()

}
