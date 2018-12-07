/*
 * Copyright (C) 2009-2018 Lightbend Inc. <https://www.lightbend.com>
 */
package docs.home.persistence

// #imports
import akka.NotUsed
import akka.stream.alpakka.couchbase.scaladsl.CouchbaseSession
import com.lightbend.lagom.scaladsl.api.{Service, ServiceCall}
import com.lightbend.lagom.scaladsl.persistence.ReadSide
import com.lightbend.lagom.scaladsl.persistence.couchbase.CouchbaseReadSide
import docs.home.persistence.CouchbaseReadSideProcessorTwo.HelloEventProcessor
import play.api.libs.json.{Format, Json}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

// #imports

object CouchbaseReadSideQuery {

  trait GreetingService extends Service {
    def userGreetings(): ServiceCall[NotUsed, List[UserGreeting]]

    override final def descriptor = {
      import Service._
      named("hello")
        .withCalls(
          pathCall("/api/user-greetings/", userGreetings _)
        )
        .withAutoAcl(true)
    }
  }

  case class UserGreeting(name: String, message: String)
  object UserGreeting {
    implicit val format: Format[UserGreeting] = Json.format[UserGreeting]
  }

  // #service-impl
  // FIXME both ReadSide and CouchbaseReadSide what is wrong here?
  class GreetingServiceImpl(readSide: CouchbaseReadSide with ReadSide, session: CouchbaseSession)(
      implicit ec: ExecutionContext
  ) extends GreetingService {
    readSide.register[HelloEvent](new HelloEventProcessor(readSide))
    override def userGreetings() =
      ServiceCall { request =>
        session.get("users-actual-greetings").map {
          case Some(jsonDoc) =>
            val json = jsonDoc.content()
            json.getNames().asScala.map(name => UserGreeting(name, json.getString(name))).toList
          case None => List.empty[UserGreeting]
        }
      }
  }
  // #service-impl

  //#register-event-processor
  ???
  //#register-event-processor
}
