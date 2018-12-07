/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package docs.home.persistence
import com.lightbend.lagom.scaladsl.persistence.{AggregateEvent, AggregateEventShards, AggregateEventTag}

object HelloEvent {
  val NumShards = 20
  val Tag = AggregateEventTag.sharded[HelloEvent](NumShards)

  final case class GreetingChanged(name: String, message: String) extends HelloEvent
}

sealed trait HelloEvent extends AggregateEvent[HelloEvent] {
  override def aggregateTag: AggregateEventShards[HelloEvent] = HelloEvent.Tag
}
