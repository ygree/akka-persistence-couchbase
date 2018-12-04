/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package akka.persistence.couchbase.internal

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import akka.annotation.InternalApi
import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.couchbase.client.java.AsyncBucket
import com.couchbase.client.java.query.{AsyncN1qlQueryResult, AsyncN1qlQueryRow, N1qlQuery}
import rx.functions.Func1
import rx.{Observable, Subscriber}

import scala.concurrent.duration._

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object N1qlQueryStage {
  private case object Poll

  case class N1qlQuerySettings(liveQueryInterval: FiniteDuration, pageSize: Int)

  private sealed trait InternalState
  private final case object Idle extends InternalState
  private final case object IdleAfterFullPage extends InternalState
  private final case class Querying(subscriber: Subscriber[AsyncN1qlQueryRow]) extends InternalState

  private val unfoldRows = new Func1[AsyncN1qlQueryResult, Observable[AsyncN1qlQueryRow]] {
    def call(t: AsyncN1qlQueryResult): Observable[AsyncN1qlQueryRow] =
      t.rows()
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class N1qlQueryStage[S](live: Boolean,
                                            settings: N1qlQueryStage.N1qlQuerySettings,
                                            initialQuery: N1qlQuery,
                                            bucket: AsyncBucket,
                                            initialState: S,
                                            nextQuery: S => Option[N1qlQuery],
                                            updateState: (S, AsyncN1qlQueryRow) => S)
    extends GraphStage[SourceShape[AsyncN1qlQueryRow]] {

  import N1qlQueryStage._

  val out: Outlet[AsyncN1qlQueryRow] = Outlet("LiveN1qlQuery.out")
  override def shape: SourceShape[AsyncN1qlQueryRow] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogicWithLogging(shape) with OutHandler {
      private var currentState: S = initialState
      private var rowsInCurrentQuery = 0
      private val buffer = new util.ArrayDeque[AsyncN1qlQueryRow]
      private var state: InternalState = Idle

      private val newRowCb = getAsyncCallback[AsyncN1qlQueryRow] { row =>
        currentState = updateState(currentState, row)
        log.debug("New row: {}. Updating state to {}", row, currentState)
        rowsInCurrentQuery += 1
        buffer.push(row)

        tryPush()
      }

      private val completeCb = getAsyncCallback[Unit] { _ =>
        log.debug("Query complete. Remaining buffer: {}, rowsInCurrentQuery: {}, pageSize: {}",
                  buffer,
                  rowsInCurrentQuery,
                  settings.pageSize)
        if (rowsInCurrentQuery == settings.pageSize)
          state = IdleAfterFullPage
        else
          state = Idle
        if (buffer.isEmpty) {
          if (live) {
            // continue until we don't get a full page
            // TODO alternative would be to more aggressively query next until we get empty result
            if (rowsInCurrentQuery == settings.pageSize)
              doNextQuery()
            else {
              // wait for timer
            }
          } else {
            // non-live, continue until we don't get a full page
            if (rowsInCurrentQuery == settings.pageSize)
              doNextQuery()
            else
              completeStage()
          }
        } else {
          tryPush()
        }
        rowsInCurrentQuery = 0
      }

      private val failedCb = getAsyncCallback[Throwable] { t =>
        log.error(t, "Query failed")
        failStage(t)
      }

      override def preStart(): Unit = {
        if (live)
          schedulePeriodicallyWithInitialDelay(Poll, settings.liveQueryInterval, settings.liveQueryInterval)
        executeQuery(initialQuery)
      }

      override protected def onTimer(timerKey: Any): Unit = timerKey match {
        case Poll =>
          state match {
            case Idle | IdleAfterFullPage =>
              log.debug("Poll when idle")
              if (buffer.isEmpty) {
                doNextQuery()
              } else {
                tryPush()
              }
            case Querying(_) =>
              log.debug("Query already outstanding. Ignoring poll.")
          }
      }

      private def doNextQuery(): Unit = {
        require(state == Idle || state == IdleAfterFullPage)
        nextQuery(currentState) match {
          case Some(next) =>
            log.debug("doNextQuery {}", next)
            executeQuery(next)

          case None =>
            log.debug("doNextQuery - finished")
            completeStage()
        }
      }

      private def executeQuery(query: N1qlQuery): Unit = {
        val subscriber = new Subscriber[AsyncN1qlQueryRow]() {
          override def onCompleted(): Unit = completeCb.invoke(())
          override def onError(t: Throwable): Unit = failedCb.invoke(t)
          override def onNext(row: AsyncN1qlQueryRow): Unit = newRowCb.invoke(row)
        }
        state = Querying(subscriber)
        log.debug("Executing query {}", query)
        bucket
          .query(query)
          .flatMap(N1qlQueryStage.unfoldRows)
          .subscribe(subscriber)
      }

      override def onPull(): Unit = {
        log.debug("onPull {}", buffer)
        tryPush()

        if (!live && buffer.isEmpty) {
          state match {
            case IdleAfterFullPage =>
              doNextQuery()
            case Idle =>
              completeStage
            case Querying(_) => // more in flight
          }
        }
      }

      private def tryPush(): Unit = {
        log.debug("tryPush {}", buffer)
        if (isAvailable(out)) {
          val row = buffer.pollLast()
          if (row ne null) push(out, row)
        }
      }

      override def postStop(): Unit =
        state match {
          case Querying(subscriber) => subscriber.unsubscribe()
          case _ =>
        }

      setHandler(out, this)
    }

}
