package akka.stream.alpakka.couchbase.scaladsl
import akka.NotUsed
import akka.dispatch.ExecutionContexts
import akka.stream.alpakka.couchbase.CouchbaseSessionSettings
import akka.stream.scaladsl.Source
import com.couchbase.client.java.Bucket

import scala.concurrent.Future
import scala.util.Success

object Couchbase {
  def apply(settings: CouchbaseSessionSettings, bucketName: String): Couchbase =
    new Couchbase(CouchbaseSession.apply(settings, bucketName))

  def apply(bucket: Bucket): Couchbase =
    new Couchbase(Future.successful(CouchbaseSession.apply(bucket)))
}

class Couchbase private (couchbase: Future[CouchbaseSession]) {
  //TODO: is ExecutionContexts.sameThreadExecutionContext fine or should we pass execution context?

  def mapToFuture[A](f: CouchbaseSession => Future[A]): Future[A] =
    couchbase.value match {
      case Some(Success(c)) => f(c)
      case _ => couchbase.flatMap(f)(ExecutionContexts.sameThreadExecutionContext)
    }

  def mapToSource[Out](f: CouchbaseSession => Source[Out, NotUsed]): Source[Out, NotUsed] =
    couchbase.value match {
      case Some(Success(c)) => f(c)
      case _ =>
        Source.fromFuture(couchbase).flatMapConcat(f) //TODO: is there a way to preserve Mat type of f here?
    }

  def close(): Unit =
    //leaving it closing behind for now
    couchbase.foreach(_.close())(ExecutionContexts.sameThreadExecutionContext)

}
