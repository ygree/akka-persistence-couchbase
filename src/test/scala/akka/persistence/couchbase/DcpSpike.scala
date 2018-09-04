package akka.persistence.couchbase

import java.util.concurrent.TimeUnit

import com.couchbase.client.dcp.transport.netty.ChannelFlowController
import com.couchbase.client.dcp._
import com.couchbase.client.deps.io.netty.buffer.ByteBuf
import com.couchbase.client.dcp.message.DcpDeletionMessage
import com.couchbase.client.dcp.message.DcpMutationMessage
import com.couchbase.client.deps.io.netty.util.CharsetUtil

import scala.io.StdIn

object DcpSpike extends App {

  val client = Client.configure()
    .credentialsProvider(new StaticCredentialsProvider("admin", "admin1"))
    .hostnames("localhost")
    .bucket("akka")
    .build()


  client.controlEventHandler(new ControlEventHandler {
    override def onEvent(flowController: ChannelFlowController, event: ByteBuf): Unit = {
      println(s"$flowController $event")
      event.release()
    }
  })

  client.dataEventHandler(new DataEventHandler {
    override def onEvent(flowController: ChannelFlowController, event: ByteBuf): Unit = {
      if (DcpMutationMessage.is(event)) {
        println("Mutation: " + DcpMutationMessage.toString(event))
        println(DcpMutationMessage.content(event).toString(CharsetUtil.UTF_8))
      }
      else if (DcpDeletionMessage.is(event)) {
//        System.out.println("Deletion: " + DcpDeletionMessage.toString(event))
      }
      else {
        println("Got: " + flowController + " " + event)
      }
      event.release()
    }
  })

  client.connect().await()
  client.initializeState(StreamFrom.BEGINNING, StreamTo.INFINITY).await()

  client.startStreaming().await()
  println("Started streaming")

  StdIn.readLine()

  client.disconnect().await()

}
