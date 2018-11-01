/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.scaladsl.persistence.couchbase

import akka.Done
import akka.actor.ActorSystem
import com.couchbase.client.java.AsyncBucket
import com.lightbend.lagom.scaladsl.persistence.ReadSideProcessor.ReadSideHandler
import com.lightbend.lagom.scaladsl.persistence.{ AggregateEventTag, EventStreamElement, ReadSideProcessor, TestEntity }
import akka.persistence.couchbase._
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.document.JsonDocument
import com.lightbend.lagom.internal.persistence.couchbase.CouchbaseAction

import scala.collection.immutable
import scala.concurrent.{ ExecutionContext, Future }

object TestEntityReadSide {
  class TestEntityReadSideProcessor(system: ActorSystem, readSide: CouchbaseReadSide, session: AsyncBucket) extends ReadSideProcessor[TestEntity.Evt] {

    def buildHandler(): ReadSideHandler[TestEntity.Evt] = {
      import system.dispatcher

      def updateCount(element: EventStreamElement[TestEntity.Appended]): Future[immutable.Seq[CouchbaseAction]] = {
        Future.successful(immutable.Seq(new CouchbaseAction {
          override def execute(ab: AsyncBucket, ex: ExecutionContext): Future[Done] = {
            getCount(ab, element.entityId).flatMap((count: Long) => {
              singleObservableToFuture(ab.upsert(JsonDocument.create(s"count-${element.entityId}", JsonObject.create().put("count", count + 1))))
            }).map(_ => Done)
          }
        }))
      }

      readSide.builder[TestEntity.Evt]("testoffsets")
        .setEventHandler[TestEntity.Appended](updateCount)
        .build()
    }

    def aggregateTags: Set[AggregateEventTag[TestEntity.Evt]] = TestEntity.Evt.aggregateEventShards.allTags

  }

  def getCount(session: AsyncBucket, entityId: String)(implicit ec: ExecutionContext): Future[Long] = {
    zeroOrOneObservableToFuture(session.get(s"count-$entityId")).map {
      case Some(l) => l.content().getLong("count")
      case None    => 0L
    }
  }

}

class TestEntityReadSide(system: ActorSystem, session: AsyncBucket) {

  import system.dispatcher

  def getAppendCount(entityId: String): Future[Long] = {
    TestEntityReadSide.getCount(session, entityId)
  }
}
