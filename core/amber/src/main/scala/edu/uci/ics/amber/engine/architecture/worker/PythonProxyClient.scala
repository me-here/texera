package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.{
  DataSendingPolicy,
  OneToOnePolicy,
  RoundRobinPolicy
}
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy2
import edu.uci.ics.amber.engine.architecture.worker.PythonProxyClient.communicate
import edu.uci.ics.amber.engine.architecture.worker.WorkerBatchInternalQueue.{
  ControlElement,
  DataElement
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddOutputPolicyHandler.AddOutputPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  DataFrame,
  DataPayload,
  EndOfUpstream
}
import edu.uci.ics.amber.engine.common.ambermessage2.WorkflowControlMessage
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, ambermessage2}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}
import org.apache.arrow.flight._
import org.apache.arrow.flight.example.InMemoryStore
import org.apache.arrow.memory.{BufferAllocator, RootAllocator}
import org.apache.arrow.vector.types.FloatingPointPrecision
import org.apache.arrow.vector.types.pojo.ArrowType.ArrowTypeID
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, Schema}
import org.apache.arrow.vector.{
  BitVector,
  FieldVector,
  Float8Vector,
  IntVector,
  VarCharVector,
  VectorSchemaRoot
}

import java.nio.charset.StandardCharsets
import java.util
import scala.collection.mutable

object MSG extends Enumeration {
  type MSGType = Value
  val HEALTH_CHECK: Value = Value
}

object PythonProxyClient {

  private def communicate(client: FlightClient, message: String): Array[Byte] =
    client.doAction(new Action(message)).next.getBody
}

case class PythonProxyClient(portNumber: Int, operator: IOperatorExecutor)
    extends Runnable
    with AutoCloseable
    with WorkerBatchInternalQueue {

  val allocator: BufferAllocator =
    new RootAllocator().newChildAllocator("flight-server", 0, Long.MaxValue);
  val location: Location = Location.forGrpcInsecure("localhost", portNumber)
  val mem: InMemoryStore = new InMemoryStore(allocator, location)

  private val MAX_TRY_COUNT: Int = 3
  private val WAIT_TIME_MS = 1000
  var schemaRoot: VectorSchemaRoot = null
  private var flightClient: FlightClient = null

  def sendBatch(dataPayload: DataPayload, from: ActorVirtualIdentity): Any = {
    println("python-java enqueue " + dataPayload)
    enqueueData(DataElement(dataPayload, from))

  }

  override def run(): Unit = {
    connect()
    mainLoop()
  }

  def connect(): Unit = {
    var connected = false
    var tryCount = 0
    while ({
      !connected && tryCount < MAX_TRY_COUNT
    }) try {
      println("trying to connect to " + location)
      flightClient = FlightClient.builder(allocator, location).build()
      connected =
        new String(communicate(flightClient, "health_check"), StandardCharsets.UTF_8) == "ack"
      if (!connected) Thread.sleep(WAIT_TIME_MS)
    } catch {
      case e: FlightRuntimeException =>
        System.out.println("Flight CLIENT:\tNot connected to the server in this try.")
        flightClient.close()
        Thread.sleep(WAIT_TIME_MS)
        tryCount += 1
    }
    if (tryCount == MAX_TRY_COUNT)
      throw new RuntimeException(
        "Exceeded try limit of " + MAX_TRY_COUNT + " when connecting to Flight Server!"
      )
  }

  def mainLoop(): Unit = {
    while (true) {

      getElement match {
        case DataElement(dataPayload, from) =>
          println("java got a dataPayload " + dataPayload)
          dataPayload match {
            case DataFrame(frame) =>
              val tuples = mutable.Queue(frame.map((t: ITuple) => t.asInstanceOf[Tuple]): _*)
              writeArrowStream(flightClient, tuples, 100, from)
            case EndOfUpstream() =>
              println("JAVA receives EndOfUpstream from " + from)
              val q = mutable.Queue(
                Tuple
                  .newBuilder(
                    edu.uci.ics.texera.workflow.common.tuple.schema.Schema.newBuilder().build()
                  )
                  .build()
              )
              writeArrowStream(flightClient, q, 100, from)
//              streamWriterMap(from).completed()
          }
        case ControlElement(cmd, from) =>
          sendControl(cmd, from)

      }
    }

  }

  def sendControl(cmd: ControlPayload, from: ActorVirtualIdentity): Unit = {
    cmd match {
      case ControlInvocation(commandID: Long, command: ControlCommand[_]) => {
        command match {
          case AddOutputPolicy(policy: DataSendingPolicy) =>
            var protobufPolicy: datatransferpolicy2.DataSendingPolicy = null

            policy match {
              case _: OneToOnePolicy =>
                protobufPolicy = datatransferpolicy2.OneToOnePolicy(
                  Option(policy.policyTag),
                  policy.batchSize,
                  policy.receivers
                )

              case _: RoundRobinPolicy =>
                protobufPolicy = datatransferpolicy2.RoundRobinPolicy(
                  Option(policy.policyTag),
                  policy.batchSize,
                  policy.receivers
                )
              case _ => throw new UnsupportedOperationException("not supported data policy")
            }

            val protobufCommand = promisehandler2.AddOutputPolicy(protobufPolicy)
            val controlMessage = toWorkflowControlMessage2(from, commandID, protobufCommand)
            val action: Action = new Action("control", controlMessage.toByteArray)
            println(flightClient.doAction(action).next())
          case StartWorker() =>
          case UpdateInputLinking(identifier, inputLink) => {
            val protobufCommand = promisehandler2.UpdateInputLinking(
              identifier = identifier,
              inputLink = Option(inputLink)
            )
            val controlMessage = toWorkflowControlMessage2(from, commandID, protobufCommand)
            val action: Action = new Action("control", controlMessage.toByteArray)
            println(flightClient.doAction(action).next())

          }
          case QueryStatistics() =>
            val protobufCommand = promisehandler2.QueryStatistics()
            val controlMessage = toWorkflowControlMessage2(from, commandID, protobufCommand)
            val action: Action = new Action("control", controlMessage.toByteArray)
            println(flightClient.doAction(action).next())
        }

      }
    }
  }

  def toWorkflowControlMessage2(
      from: ActorVirtualIdentity,
      commandID: Long,
      controlCommand: promisehandler2.ControlCommand
  ): WorkflowControlMessage = {
    toWorkflowControlMessage2(from, toControlInvocation2(commandID, controlCommand))
  }

  def toWorkflowControlMessage2(
      from: ActorVirtualIdentity,
      controlPayload: ambermessage2.ControlPayload
  ): WorkflowControlMessage = {
    ambermessage2.WorkflowControlMessage(
      from = from,
      sequenceNumber = 0L,
      payload = controlPayload
    )
  }

  def toControlInvocation2(
      commandID: Long,
      controlCommand: promisehandler2.ControlCommand
  ): ambermessage2.ControlInvocation = {
    ambermessage2.ControlInvocation(commandID = commandID, command = controlCommand)
  }

  /**
    * For every batch, the operator converts list of {@code Tuple}s into Arrow stream data in almost the exact same
    * way as it would when using Arrow file, except now it sends stream to the server with
    * {@link FlightClient# startPut ( FlightDescriptor, VectorSchemaRoot, FlightClient.PutListener, CallOption...)} and
    * {@link FlightClient.ClientStreamListener# putNext ( )}. The server uses {@code do_put()} to receive data stream
    * and convert it into a {@code pyarrow.Table} and store it in the server.
    * {@code startPut} is a non-blocking call, but this method in general is a blocking call, it waits until all the
    * data are sent.
    *
    * @param client      The FlightClient that manages this.
    * @param values      The input queue that holds tuples, its contents will be consumed in this method.
    * @param arrowSchema Input Arrow table schema. This should already have been defined (converted).
    * @param chunkSize   The chunk size of the arrow stream. This is different than the batch size of the operator,
    *                    although they may seem similar. This doesn't actually affect serialization speed that much,
    *                    so in general it can be the same as {@code batchSize}.
    */
  @throws[RuntimeException]
  private def writeArrowStream(
      client: FlightClient,
      values: mutable.Queue[Tuple],
      chunkSize: Int = 100,
      from: ActorVirtualIdentity
  ): Unit = {

    println(" NOW before writing A DATA BATCH " + chunkSize + " from " + from.asMessage.toString)
    if (values.nonEmpty) {
      val cachedTuple = values.front
      val schema = cachedTuple.getSchema
      val arrowSchema = convertAmber2ArrowSchema(schema)
      val flightListener = new SyncPutListener

      val schemaRoot = VectorSchemaRoot.create(arrowSchema, allocator)
      val writer =
        client.startPut(
          FlightDescriptor.command(from.asMessage.toByteArray),
          schemaRoot,
          flightListener
        )

      try {
        while (values.nonEmpty) {

          schemaRoot.allocateNew()
          var indexThisChunk = 0
          while ({
            indexThisChunk < chunkSize && values.nonEmpty
          }) {
            convertAmber2ArrowTuple(values.dequeue(), indexThisChunk, schemaRoot)
            indexThisChunk += 1
          }
          schemaRoot.setRowCount(indexThisChunk)
          writer.putNext()
          schemaRoot.clear()
        }
        writer.completed()
        flightListener.getResult()
        flightListener.close()
        schemaRoot.clear()

      } catch {
        case e: Exception =>
          e.printStackTrace()
        //        closeAndThrow(client, e)
      }
    }

    println(" NOW WRITING A DATA BATCH " + chunkSize + " from " + from.asMessage.toString)

  }

  /**
    * Does the actual conversion (serialization) of data tuples. This is a tuple-by-tuple method, because this method
    * will be used in different places.
    *
    * @param tuple            Input tuple.
    * @param index            Index of the input tuple in the table (buffer).
    * @param vectorSchemaRoot This should store the Arrow schema, which should already been converted from Amber.
    */ @throws[ClassCastException]
  private def convertAmber2ArrowTuple(
      tuple: Tuple,
      index: Int,
      vectorSchemaRoot: VectorSchemaRoot
  ): Unit = {
    val preDefinedFields = vectorSchemaRoot.getSchema.getFields
    for (i <- 0 until preDefinedFields.size) {
      val vector: FieldVector = vectorSchemaRoot.getVector(i)
      preDefinedFields.get(i).getFieldType.getType.getTypeID match {
        case ArrowTypeID.Int =>
          vector.asInstanceOf[IntVector].set(index, tuple.get(i).asInstanceOf[Int])

        case ArrowTypeID.Bool =>
          vector
            .asInstanceOf[BitVector]
            .set(
              index,
              if (tuple.get(i).asInstanceOf[Boolean]) 1
              else 0
            )

        case ArrowTypeID.FloatingPoint =>
          vector.asInstanceOf[Float8Vector].set(index, tuple.get(i).asInstanceOf[Double])

        case ArrowTypeID.Utf8 =>
          vector
            .asInstanceOf[VarCharVector]
            .set(index, tuple.get(i).toString.getBytes(StandardCharsets.UTF_8))

      }
    }
  }

  /**
    * Converts an Amber schema into Arrow schema.
    *
    * @param amberSchema The Amber Tuple Schema.
    * @return An Arrow {@link org.apache.arrow.vector.types.pojo.Schema}.
    */ @throws[RuntimeException]
  private def convertAmber2ArrowSchema(
      amberSchema: edu.uci.ics.texera.workflow.common.tuple.schema.Schema
  ): Schema = {
    val arrowFields = new util.ArrayList[Field]
    import scala.collection.JavaConversions._
    for (amberAttribute: Attribute <- amberSchema.getAttributes) {
      val name = amberAttribute.getName
      var field: Field = null
      amberAttribute.getType match {
        case AttributeType.INTEGER =>
          field = Field.nullablePrimitive(name, new ArrowType.Int(32, true))

        case AttributeType.LONG =>
          field = Field.nullablePrimitive(name, new ArrowType.Int(64, true))

        case AttributeType.DOUBLE =>
          field = Field.nullablePrimitive(
            name,
            new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)
          )

        case AttributeType.BOOLEAN =>
          field = Field.nullablePrimitive(name, ArrowType.Bool.INSTANCE)

        case AttributeType.STRING | AttributeType.ANY =>
          field = Field.nullablePrimitive(name, ArrowType.Utf8.INSTANCE)

        case _ =>
          throw new RuntimeException("Unexpected value: " + amberAttribute.getType)
      }
      arrowFields.add(field)
    }
    new Schema(arrowFields)
  }

  override def close(): Unit = ???
}
