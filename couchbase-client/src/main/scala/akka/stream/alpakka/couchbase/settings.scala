/**
 * Copyright (C) 2009-2018 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase

import com.couchbase.client.java.env.CouchbaseEnvironment
import com.couchbase.client.java.{ PersistTo, ReplicateTo }
import com.typesafe.config.Config

import scala.collection.JavaConverters._
import scala.collection.immutable

object CouchbaseWriteSettings {

  val default = apply()

  // FIXME default params in public API is problematic for bincomp
  def apply(
    parallelism: Int                           = 1,
    replicateTo: ReplicateTo                   = ReplicateTo.ONE,
    persistTo:   PersistTo                     = PersistTo.NONE,
    timeout:     Long                          = 2L,
    timeUnit:    java.util.concurrent.TimeUnit = java.util.concurrent.TimeUnit.SECONDS): CouchbaseWriteSettings =
    new CouchbaseWriteSettings(parallelism, replicateTo, persistTo, timeout, timeUnit)

  def create(): CouchbaseWriteSettings = CouchbaseWriteSettings()

}

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

  override def toString: String = s"CouchbaseWriteSettings($parallelism, $replicateTo, $persistTo, $timeout, $timeUnit)"
}

object CouchbaseSessionSettings {

  def apply(config: Config): CouchbaseSessionSettings = {
    // FIXME environment from config
    val username = config.getString("username")
    val password = config.getString("password")
    val nodes = config.getStringList("nodes").asScala.toList
    new CouchbaseSessionSettings(username, password, nodes, None)
  }

  def apply(username: String, password: String): CouchbaseSessionSettings = {
    new CouchbaseSessionSettings(username, password, Nil, None)
  }

  def create(username: String, password: String): CouchbaseSessionSettings = {
    apply(username, password)
  }

}

final class CouchbaseSessionSettings private (
  val username:    String,
  val password:    String,
  val nodes:       immutable.Seq[String],
  val environment: Option[CouchbaseEnvironment]) {

  def withUsername(username: String): CouchbaseSessionSettings =
    copy(username = username)

  def withPassword(password: String): CouchbaseSessionSettings =
    copy(password = password)

  def withNodes(nodes: String): CouchbaseSessionSettings =
    copy(nodes = nodes :: Nil)

  def withNodes(nodes: immutable.Seq[String]): CouchbaseSessionSettings =
    copy(nodes = nodes)

  def withNodes(nodes: java.util.List[String]): CouchbaseSessionSettings =
    copy(nodes = nodes.asScala.toList)

  def withEnvironment(environment: CouchbaseEnvironment): CouchbaseSessionSettings =
    copy(environment = Some(environment))

  private def copy(
    username:    String                       = username,
    password:    String                       = password,
    nodes:       immutable.Seq[String]        = nodes,
    environment: Option[CouchbaseEnvironment] = environment): CouchbaseSessionSettings =
    new CouchbaseSessionSettings(username, password, nodes, environment)

  override def equals(other: Any): Boolean = other match {
    case that: CouchbaseSessionSettings =>
      username == that.username &&
        password == that.password &&
        nodes == that.nodes &&
        environment == that.environment
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(username, password, nodes, environment)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"CouchbaseSessionSettings($username, *****, $nodes, $environment)"
}