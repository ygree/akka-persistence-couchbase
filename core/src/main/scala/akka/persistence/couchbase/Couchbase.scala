/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase

import akka.NotUsed
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.Source
import com.couchbase.client.java.Bucket
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

object Couchbase {
  private val log = LoggerFactory.getLogger(this.getClass)

  def apply(bucket: Bucket)(implicit ec: ExecutionContext): Couchbase =
    new Couchbase(Future.successful(CouchbaseSession.apply(bucket)))

  def apply(settings: CouchbaseSessionSettings, bucketName: String, indexAutoCreate: Boolean)(
      implicit ec: ExecutionContext
  ): Couchbase = Couchbase.apply(settings, bucketName, init(indexAutoCreate) _)

  private def init(indexAutoCreate: Boolean)(session: CouchbaseSession)(implicit ec: ExecutionContext): Future[Unit] =
    if (!indexAutoCreate)
      Future(log.debug("Skipping index-autocreate!"))
    else {
      CouchbaseSchema
        .createPi2Index(session)
        .map { hasBeenCreated =>
          log.warn("index-autocreate is enabled. DON'T USE IT IN PRODUCTION!")
          if (hasBeenCreated)
            log.info("Index `pi2` has been created automatically.")
          else
            log.debug("Index `pi2` already exists.")
        }
    }

  private def apply(settings: CouchbaseSessionSettings, bucketName: String, init: CouchbaseSession => Future[Unit])(
      implicit ec: ExecutionContext
  ): Couchbase = {
    val session: Future[CouchbaseSession] = CouchbaseSession
      .apply(settings, bucketName)
      .flatMap(s => {
        init(s).map(_ => s)
      })
    new Couchbase(session)
  }
}

class Couchbase private (val underlying: Future[CouchbaseSession])(implicit ec: ExecutionContext) {

  def mapToFuture[A](f: CouchbaseSession => Future[A]): Future[A] =
    underlying.value match {
      case Some(Success(c)) => f(c)
      case _ => underlying.flatMap(f)
    }

  def mapToSource[Out](f: CouchbaseSession => Source[Out, NotUsed]): Source[Out, NotUsed] =
    underlying.value match {
      case Some(Success(c)) => f(c)
      case _ =>
        Source.fromFuture(underlying).flatMapConcat(f) //TODO: is there a way to preserve Mat type of f here?
    }

  def close(): Unit =
    //leaving it closing behind for now
    underlying.foreach(_.close())

}
