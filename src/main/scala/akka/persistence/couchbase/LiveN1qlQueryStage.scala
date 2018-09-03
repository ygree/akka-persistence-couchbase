package akka.persistence.couchbase

import akka.stream.stage._
import akka.stream.{Attributes, Outlet, SourceShape}
import com.couchbase.client.java.AsyncBucket
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{AsyncN1qlQueryRow, N1qlQuery}
import rx.Subscriber

import scala.concurrent.duration._

object LiveN1qlQueryStage {
  trait Control
  case object Poll

  sealed trait InternalState
  final case object Idle extends InternalState
  final case object Querying extends InternalState
}

// TODO pagination
class LiveN1qlQueryStage[S](initialQuery: N1qlQuery, namedParams: JsonObject, bucket: AsyncBucket, initialState: S, nextQuery: S => Option[N1qlQuery], updateState: (S, AsyncN1qlQueryRow) => S)
  extends GraphStageWithMaterializedValue[SourceShape[AsyncN1qlQueryRow], LiveN1qlQueryStage.Control] {

  import LiveN1qlQueryStage._

  val out: Outlet[AsyncN1qlQueryRow] = Outlet("LiveN1qlQuery.out")
  override def shape: SourceShape[AsyncN1qlQueryRow] = SourceShape(out)

  class LiveN1qlQueryStageLogic extends TimerGraphStageLogicWithLogging(shape) with OutHandler with Control {
    var currentState: S = initialState
    var rowsInCurrentQuery = 0
    var buffer = Vector.empty[AsyncN1qlQueryRow]
    var state: InternalState = Idle

    private val newRowCb = getAsyncCallback[AsyncN1qlQueryRow] { row =>
      currentState = updateState(currentState, row)
      log.debug("New row: {}. Updating state to {}", row, currentState)
      rowsInCurrentQuery += 1
      buffer = buffer :+ row
      tryPush()
    }

    private val completeCb = getAsyncCallback[Unit] { _ =>
      log.debug("Query complete. Remaining buffer: {}", buffer)
      state = Idle
      if (buffer.isEmpty) {
        if (rowsInCurrentQuery > 0) {
          // if rows then kick off the next query
          doNextQuery()
        } else {
          // if no rows from last, wait for timer
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
      // TODO make configurable
      schedulePeriodicallyWithInitialDelay(Poll, 1.second, 1.second)
      executeQuery(initialQuery)
    }


    override protected def onTimer(timerKey: Any): Unit = timerKey match {
      case Poll =>
        state match {
          case Idle =>
            log.debug("Poll when idle")
            if (buffer.isEmpty) {
              doNextQuery()
            } else {
              tryPush()
            }
          case Querying =>
            log.debug("Query already outstanding. Ignoring poll.")
        }
    }

    private def doNextQuery(): Unit = {
      require(state == Idle)
      state = Querying
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
      // FIXME deal with initial errors
      bucket.query(query).flatMap(result => result.rows()).subscribe(new Subscriber[AsyncN1qlQueryRow]() {
        override def onCompleted(): Unit = completeCb.invoke(())
        override def onError(t: Throwable): Unit = failedCb.invoke(t)
        override def onNext(row: AsyncN1qlQueryRow): Unit = newRowCb.invoke(row)
      })
    }

    override def onPull(): Unit = {
      log.debug("onPull {}", buffer)
      tryPush()
    }

    private def tryPush(): Unit = {
      log.debug("tryPush {}", buffer)
      if (isAvailable(out)) {
        buffer match {
          case head +: tail =>
            push(out, head)
            buffer = tail
          case _ => // wait for next row
        }
      }
    }

    setHandler(out, this)
  }


  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, LiveN1qlQueryStage.Control) = {
    val logic = new LiveN1qlQueryStageLogic()
    (logic, logic)
  }
}
