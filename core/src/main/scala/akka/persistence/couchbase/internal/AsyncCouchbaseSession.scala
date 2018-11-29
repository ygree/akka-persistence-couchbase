/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.internal

import akka.annotation.InternalApi
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import akka.stream.scaladsl.Source
import akka.{Done, NotUsed}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

/**
 * INTERNAL API
 *
 * Wrapper for dealing with the fact that the Couchbase session is created asynchronously
 */
@InternalApi
private[akka] trait AsyncCouchbaseSession {

  /**
   * Note: Implement with a val so that it doesn't get recreated on each access
   */
  protected def asyncSession: Future[CouchbaseSession]

  // optimizations avoiding an extra execution context jump when the session is already created
  // (most of the time after startup completed)
  final def withCouchbaseSession[A](f: CouchbaseSession => Future[A])(implicit ec: ExecutionContext): Future[A] =
    asyncSession.value match {
      case Some(Success(c)) => f(c)
      case _ => asyncSession.flatMap(f)
    }

  final def sourceWithCouchbaseSession[Out](
      f: CouchbaseSession => Source[Out, NotUsed]
  )(implicit ec: ExecutionContext): Source[Out, NotUsed] =
    asyncSession.value match {
      case Some(Success(c)) => f(c)
      case _ => Source.fromFutureSource(asyncSession.map(f)).mapMaterializedValue(_ => NotUsed)
    }

  final def closeCouchbaseSession()(implicit ec: ExecutionContext): Future[Done] =
    //leaving it closing behind for now
    asyncSession.value match {
      case Some(Success(session)) => session.close()
      case _ => asyncSession.flatMap(_.close())
    }

}
