package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.common.ambermessage2.WorkflowControlMessage
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ReturnPayload
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.CommandCompleted
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager._
import kotlin.NotImplementedError
import org.apache.arrow.flight._
import org.apache.arrow.flight.example.InMemoryStore
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.util.AutoCloseables

private class AmberProducer(
    allocator: BufferAllocator,
    location: Location,
    controlOutputPort: ControlOutputPort
) extends InMemoryStore(allocator, location) {

  override def doAction(
      context: FlightProducer.CallContext,
      action: Action,
      listener: FlightProducer.StreamListener[Result]
  ): Unit = {
    action.getType match {
      case "control" =>
        val workflowControlMessage = WorkflowControlMessage.parseFrom(action.getBody)
        println("PythonProxyServer-JAVA received CONTROL from PYTHON " + workflowControlMessage)

        var returnValue1: Any = null
        workflowControlMessage.payload match {
          case returnPayloadV2: edu.uci.ics.amber.engine.common.ambermessage2.ReturnPayload => {
            println()

            if (returnPayloadV2.returnValue.isDefined) {
              returnPayloadV2.returnValue match {
                case workerStatistics: edu.uci.ics.amber.engine.architecture.worker.promisehandler2.WorkerStatistics => {
                  println("PythonProxyServer-JAVA this is statistics:::" + workerStatistics)
                  var state: WorkerState = null
                  workerStatistics.workerState match {
                    case _: edu.uci.ics.amber.engine.common.statetransition2.Uninitialized =>
                      state = Uninitialized
                    case _: edu.uci.ics.amber.engine.common.statetransition2.Ready =>
                      state = Ready
                    case _: edu.uci.ics.amber.engine.common.statetransition2.Running =>
                      state = Running
                    case _: edu.uci.ics.amber.engine.common.statetransition2.Paused =>
                      state = Paused
                    case _: edu.uci.ics.amber.engine.common.statetransition2.Completed =>
                      state = Completed
                    case _: edu.uci.ics.amber.engine.common.statetransition2.Recovering =>
                      state = Recovering

                  }
                  returnValue1 = WorkerStatistics(
                    state,
                    workerStatistics.inputRowCount,
                    workerStatistics.outputRowCount
                  )
                }
                case _ => returnValue1 = CommandCompleted()
              }
            } else {
              returnValue1 = CommandCompleted()
            }
          }
          case _ => returnValue1 = CommandCompleted()
        }
        println(s" PythonProxyServer-JAVA RESPONSE TO OTHER ACTORS with $returnValue1")
        controlOutputPort.sendTo(
          to = workflowControlMessage.from,
          payload = ReturnPayload(
            originalCommandID =
              workflowControlMessage.payload.asMessage.getReturnPayload.originalCommandID,
            returnValue = returnValue1
          )
        )
        listener.onNext(new Result("ack".getBytes))
        listener.onCompleted()
      case _ => throw new NotImplementedError()
    }

  }

  override def acceptPut(context: FlightProducer.CallContext, flightStream: FlightStream, ackStream: FlightProducer.StreamListener[PutResult]): Runnable = {
    println(" PythonProxyServer-JAVA got a data batch return from python!!!")
    super.acceptPut(context, flightStream, ackStream)

  }


}

class PythonProxyServer(portNumber: Int, controlOutputPort: ControlOutputPort)
    extends Runnable
    with AutoCloseable {

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
