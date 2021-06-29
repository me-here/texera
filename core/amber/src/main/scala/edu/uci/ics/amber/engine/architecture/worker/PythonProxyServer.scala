package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.ambermessage2.WorkflowControlMessage
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ReturnPayload
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.CommandCompleted
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import kotlin.NotImplementedError
import org.apache.arrow.flight._
import org.apache.arrow.flight.example.InMemoryStore
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.util.AutoCloseables


private class AmberProducer(allocator: BufferAllocator, location: Location, controlOutputPort: ControlOutputPort)
    extends InMemoryStore(allocator, location) {

  override def doAction(context: FlightProducer.CallContext, action: Action, listener: FlightProducer.StreamListener[Result]): Unit = {
    action.getType match {
      case "control" =>
        val workflowControlMessage = WorkflowControlMessage.parseFrom(action.getBody)
        println("java got!!!" + workflowControlMessage)
        println(CONTROLLER == workflowControlMessage.from)
        println(workflowControlMessage.payload)


        controlOutputPort.sendTo(to = workflowControlMessage.from, payload = ReturnPayload(originalCommandID = workflowControlMessage.payload.asMessage.getReturnPayload.originalCommandID, returnValue = CommandCompleted()))
        listener.onNext(new Result("ack".getBytes))
        listener.onCompleted()
      case _ => throw new NotImplementedError()
    }


  }
}

class PythonProxyServer(portNumber: Int, controlOutputPort: ControlOutputPort) extends Runnable with AutoCloseable {

  val allocator: BufferAllocator =
    new RootAllocator().newChildAllocator("flight-server", 0, Long.MaxValue);
  val location: Location = Location.forGrpcInsecure("localhost", portNumber)
  val mem: InMemoryStore = new AmberProducer(allocator, location, controlOutputPort)
  val server: FlightServer = FlightServer.builder(allocator, location, mem).build()


  override def run(): Unit = {
    server.start()
  }

  @throws[Exception]
  override def close(): Unit = {
    AutoCloseables.close(mem, server, allocator)
  }
}
