package edu.uci.ics.amber.engine.recovery

import java.io.File

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import akka.util.Timeout
import edu.uci.ics.amber.clustering.SingleNodeListener
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{NetworkAck, NetworkMessage, RegisterActorRef}
import edu.uci.ics.amber.engine.architecture.sendsemantics.datatransferpolicy.OneToOnePolicy
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddOutputPolicyHandler.AddOutputPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, ISourceOperatorExecutor, InputExhausted}
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, DPCursor, DataBatchSequence, DataFrame, EndOfUpstream, FromSender, InputLinking, WorkflowControlMessage, WorkflowDataMessage, WorkflowFIFOMessage, WorkflowMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity.WorkerActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, LinkIdentity}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration._

class RecoverySpec
    extends TestKit(ActorSystem("RecoverySpec"))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  implicit val timeout: Timeout = Timeout(5.seconds)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  def deleteFolderSafely(path: String): Unit = {
    val folder = new File(path)
    if (folder.exists()) {
      FileUtils.cleanDirectory(folder)
      FileUtils.deleteDirectory(folder)
    }
  }

  override def beforeAll: Unit = {
    deleteFolderSafely("./logs")
    system.actorOf(Props[SingleNodeListener], "cluster-info")
  }
  override def afterAll: Unit = {
    deleteFolderSafely("./logs")
    TestKit.shutdownActorSystem(system)
  }
  val receiverID = WorkerActorVirtualIdentity("receiver")
  val fakeLink: LinkIdentity =
    LinkIdentity(
      LayerIdentity("testRecovery", "mockOp", "src"),
      LayerIdentity("testRecovery", "mockOp", "dst")
    )

  class SourceOperatorForRecoveryTest(outputLimit: Int = 15, generateInterval: Int = 100)
      extends ISourceOperatorExecutor {
    override def open(): Unit = {}
    override def close(): Unit = {}
    override def produce(): Iterator[ITuple] = {
      (for (i <- (1 to outputLimit).view) yield {
        Thread.sleep(generateInterval); println(s"generating tuple $i"); ITuple(i)
      }).toIterator
    }
  }

  class DummyOperatorForRecoveryTest() extends IOperatorExecutor {
    override def open(): Unit = {}
    override def close(): Unit = {}
    override def processTuple(
        tuple: Either[ITuple, InputExhausted],
        input: LinkIdentity
    ): Iterator[ITuple] = {
      tuple match {
        case Left(value) =>
          println(s"received tuple $value")
          Iterator(value)
        case Right(value) =>
          println(s"received tuple $value")
          Iterator.empty
      }
    }
  }

  def forAllNetworkMessages(probe: TestProbe, action: (WorkflowMessage) => Unit): Unit = {
    if (probe != null) {
      probe.receiveWhile(idle = 3.seconds) {
        case NetworkMessage(id, content: WorkflowFIFOMessage) =>
          probe.sender() ! NetworkAck(id)
          action(content)
        case NetworkMessage(id, _) =>
          probe.sender() ! NetworkAck(id)
        case other => //skip
      }
    }
  }

  def initWorker(
      id: ActorVirtualIdentity,
      op: IOperatorExecutor,
      controller: TestProbe,
      actorMappingToRegister: Seq[(ActorVirtualIdentity, ActorRef)],
      logStorage: LogStorage
  ): ActorRef = {
    val worker = TestActorRef(
      new WorkflowWorker(id, op, controller.ref, logStorage) {
        networkCommunicationActor ! RegisterActorRef(
          ActorVirtualIdentity.Controller,
          controller.ref
        )
        actorMappingToRegister.foreach {
          case (id, ref) =>
            networkCommunicationActor ! RegisterActorRef(id, ref)
        }
      }
    )
    worker
  }

  def sendMessagesAsync(worker: ActorRef, controls: Seq[ControlCommand[_]]): Future[Boolean] = {
    Future {
      sendMessages(worker, controls)
      true
    }(ExecutionContext.global)
  }

  def sendMessages(worker: ActorRef, controls: Seq[ControlCommand[_]]): Unit = {
    val messages = controls.indices.map(i =>
      WorkflowControlMessage(
        ActorVirtualIdentity.Controller,
        i,
        ControlInvocation(i, controls(i))
      )
    )
    messages.foreach { x =>
      worker ! NetworkMessage(0, x)
      Thread.sleep(400)
    }
  }

  def waitResponsesAndKillWorker(
      worker: ActorRef,
      controller: TestProbe,
      receiver: TestProbe
  ): mutable.Queue[Any] = {
    val receivedMessages = mutable.Queue[Any]()
    forAllNetworkMessages(controller, x => receivedMessages.enqueue(x))
    forAllNetworkMessages(receiver, x => receivedMessages.enqueue(x))
    worker ! PoisonPill
    println("received messages: \n" + receivedMessages.mkString("\n"))
    receivedMessages
  }

  def testRecovery(
      worker: ActorRef,
      controller: TestProbe,
      receiver: TestProbe,
      receivedMessages: mutable.Queue[Any]
  ): Unit = {
    Thread.sleep(15000)
    forAllNetworkMessages(controller, x => assert(receivedMessages.dequeue() == x))
    forAllNetworkMessages(receiver, x => assert(receivedMessages.dequeue() == x))
    assert(receivedMessages.isEmpty)
  }

  def smallWorkerChain(
      sender1: ActorVirtualIdentity,
      sender2: ActorVirtualIdentity,
      sender1Log: LogStorage,
      sender2Log: LogStorage
  ): (
      ISourceOperatorExecutor,
      IOperatorExecutor,
      ActorRef,
      ActorRef,
      TestProbe,
      TestProbe,
      TestProbe
  ) = {
    val source = new SourceOperatorForRecoveryTest()
    val dummy = new DummyOperatorForRecoveryTest()
    val controller1 = TestProbe()
    val controller2 = TestProbe()
    val receiver = TestProbe()
    val controlsForSource = Seq(
      QueryStatistics(),
      AddOutputPolicy(new OneToOnePolicy(fakeLink, 1, Array(sender2))),
      StartWorker(),
      QueryStatistics(),
      QueryStatistics(),
      QueryStatistics()
    )
    val controlsForDummy = Seq(
      AddOutputPolicy(new OneToOnePolicy(fakeLink, 1, Array(receiverID))),
      QueryStatistics(),
      QueryStatistics(),
      QueryStatistics(),
      QueryStatistics()
    )
    val dummyWorker = initWorker(
      sender2,
      dummy,
      controller2,
      Seq((receiverID, receiver.ref)),
      sender2Log
    )
    val sourceWorker = initWorker(
      sender1,
      source,
      controller1,
      Seq((sender2, dummyWorker)),
      sender1Log
    )
    val f1 = sendMessagesAsync(sourceWorker, controlsForSource)
    val f2 = sendMessagesAsync(dummyWorker, controlsForDummy)
    Await.result(f1, 20.seconds)
    Await.result(f2, 20.seconds)
    (source, dummy, sourceWorker, dummyWorker, controller1, controller2, receiver)
  }
//  The following test will randomly fail in github action, the reason is still unclear.

  "worker" should "write logs during normal processing" in {
    val id = WorkerActorVirtualIdentity("testRecovery1")
    val sender1 = WorkerActorVirtualIdentity("sender1")
    val sender2 = WorkerActorVirtualIdentity("sender2")
    val sender3 = WorkerActorVirtualIdentity("sender3")
    val messages = Seq(
      WorkflowDataMessage(
        sender1,
        0,
        InputLinking(fakeLink)
      ),
      WorkflowDataMessage(
        sender2,
        0,
        InputLinking(fakeLink)
      ),
      WorkflowDataMessage(
        sender3,
        0,
        InputLinking(fakeLink)
      ),
      WorkflowDataMessage(sender1, 1, DataFrame(Array.empty)),
      WorkflowDataMessage(sender2, 1, DataFrame(Array.empty)),
      WorkflowDataMessage(sender2, 2, DataFrame(Array.empty)),
      WorkflowControlMessage(sender2, 0, ControlInvocation(-1, QueryStatistics())),
      WorkflowDataMessage(sender3, 1, DataFrame(Array.empty)),
      WorkflowControlMessage(sender3, 0, ControlInvocation(-1, QueryStatistics())),
      WorkflowDataMessage(sender1, 2, DataFrame(Array.empty))
    )
    val op = new SourceOperatorForRecoveryTest()
    val logStorage: LogStorage =
      new LocalDiskLogStorage(id.toString)
    val worker = system.actorOf(
      WorkflowWorker.props(id, op, TestProbe().ref, logStorage)
    )
    messages.foreach { x =>
      worker ! NetworkMessage(0, x)
    }
    Thread.sleep(10000)
    val logStorage2 = new LocalDiskLogStorage(id.toString)
    assert(logStorage2.getLogs.count(p => p.isInstanceOf[WorkflowControlMessage]) == 2)
    assert(logStorage2.getLogs.count(p => p.isInstanceOf[FromSender]) == 8)
    assert(logStorage2.getLogs.count(p => p.isInstanceOf[DPCursor]) == 2)
    logStorage.clear()
  }

  "source worker" should "recover with the log after restarting" in {
    val id = WorkerActorVirtualIdentity("testRecovery2")
    val sender1 = WorkerActorVirtualIdentity("sender4")
    val op = new SourceOperatorForRecoveryTest()
    val controller = TestProbe()
    val receiver = TestProbe()
    val controls = Seq(
      AddOutputPolicy(new OneToOnePolicy(fakeLink, 1, Array(receiverID))),
      StartWorker(),
      QueryStatistics(),
      QueryStatistics(),
      QueryStatistics()
    )
    val workerLog = new LocalDiskLogStorage(id.toString)
    val worker = initWorker(
      id,
      op,
      controller,
      Seq((receiverID, receiver.ref)),
      workerLog
    )
    sendMessages(worker, controls)
    val received = waitResponsesAndKillWorker(worker, controller, receiver)
    val recovered = initWorker(
      id,
      op,
      controller,
      Seq((receiverID, receiver.ref)),
      workerLog
    )
    testRecovery(recovered, controller, receiver, received)
    workerLog.clear()
  }

  "multiple workers" should "recover with their logs after restarting" in {
    val sourceID = WorkerActorVirtualIdentity("source1")
    val dummyID = WorkerActorVirtualIdentity("dummy1")
    val sourceLogStorage: LogStorage = new LocalDiskLogStorage(sourceID.toString)
    val dummyLogStorage: LogStorage = new LocalDiskLogStorage(dummyID.toString)
    val (source, dummy, sourceWorker, dummyWorker, controller1, controller2, receiver) =
      smallWorkerChain(
        sourceID,
        dummyID,
        sourceLogStorage,
        dummyLogStorage
      )
    val receivedMessageForSource =
      waitResponsesAndKillWorker(sourceWorker, controller1, null)
    val receivedMessageForDummy =
      waitResponsesAndKillWorker(dummyWorker, controller2, receiver)
    val recoveredDummy = initWorker(
      dummyID,
      dummy,
      controller2,
      Seq((receiverID, receiver.ref)),
      dummyLogStorage
    )
    val recoveredSource = initWorker(
      sourceID,
      source,
      controller1,
      Seq((dummyID, recoveredDummy)),
      sourceLogStorage
    )
    testRecovery(recoveredSource, controller1, null, receivedMessageForSource)
    testRecovery(recoveredDummy, controller2, receiver, receivedMessageForDummy)
    sourceLogStorage.clear()
    dummyLogStorage.clear()
  }

  "one worker" should "recover correctly while the other worker are still alive" in {
    val sourceID = WorkerActorVirtualIdentity("source2")
    val dummyID = WorkerActorVirtualIdentity("dummy2")
    val sourceLogStorage: LogStorage = new LocalDiskLogStorage(sourceID.toString)
    val dummyLogStorage: LogStorage = new LocalDiskLogStorage(dummyID.toString)
    val (source, dummy, sourceWorker, dummyWorker, controller1, controller2, receiver) =
      smallWorkerChain(
        sourceID,
        dummyID,
        sourceLogStorage,
        dummyLogStorage
      )
    val receivedMessageForSource =
      waitResponsesAndKillWorker(sourceWorker, controller1, null)
    val recoveredSource = initWorker(
      sourceID,
      source,
      controller1,
      Seq((dummyID, dummyWorker)),
      sourceLogStorage
    )
    testRecovery(recoveredSource, controller1, null, receivedMessageForSource)
    val expectedData =
      (Seq(WorkflowDataMessage(dummyID, 0, InputLinking(fakeLink))) ++ (1 until 16).map(x =>
        WorkflowDataMessage(dummyID, x, DataFrame(Array(ITuple(x))))
      ) ++ Seq(WorkflowDataMessage(dummyID, 16, EndOfUpstream()))).to[mutable.Queue]
    forAllNetworkMessages(receiver, w => assert(w == expectedData.dequeue()))
    val receivedControl = mutable.Queue[WorkflowMessage]()
    forAllNetworkMessages(controller2, w => receivedControl.enqueue(w))
    assert(receivedControl.size == 8)
    sourceLogStorage.clear()
    dummyLogStorage.clear()
  }

  "I " should "test this" in{
    val file = HDFSLogStorage.hdfs.create(new Path("./logs/sample1.log"))
    var sumLatency = 0L
    val content2 = Array.fill(4096)(1.byteValue())
    (0 until 20).foreach{ x =>
      (0 until 1000).foreach{
        x =>
          val start = System.currentTimeMillis()
          file.write(content2)
          file.hflush()
          sumLatency += (System.currentTimeMillis()-start)
      }
      println(sumLatency/1000f)
      sumLatency = 0
    }
  }

}
