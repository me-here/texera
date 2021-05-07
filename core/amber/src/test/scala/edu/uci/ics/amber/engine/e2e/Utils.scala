package edu.uci.ics.amber.engine.e2e

import akka.actor.{ActorRef, ActorSystem, PoisonPill, Props}
import akka.testkit.TestProbe
import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{
  Controller,
  ControllerEventListener,
  ControllerState,
  Workflow
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.workflow.{
  BreakpointInfo,
  OperatorLink,
  WorkflowCompiler,
  WorkflowInfo
}

import scala.collection.mutable
import scala.concurrent.duration._

object Utils {

  def buildWorkflow(
      operators: mutable.MutableList[OperatorDescriptor],
      links: mutable.MutableList[OperatorLink]
  ): (WorkflowIdentity, Workflow) = {
    val context = new WorkflowContext
    context.jobID = "workflow-test"

    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(operators, links, mutable.MutableList[BreakpointInfo]()),
      context
    )
    val workflow = texeraWorkflowCompiler.amberWorkflow
    val workflowTag = WorkflowIdentity("workflow-test")
    (workflowTag, workflow)
  }

  def executeWorkflow(id: WorkflowIdentity, workflow: Workflow)(implicit
      actorSystem: ActorSystem
  ): Map[String, List[ITuple]] = {
    val parent = TestProbe()
    var results: Map[String, List[ITuple]] = null
    val eventListener = ControllerEventListener()
    eventListener.workflowCompletedListener = evt => results = evt.result
    val controller = parent.childActorOf(
      Controller.props(id, workflow, eventListener, 100)
    )
    parent.expectMsg(ControllerState.Ready)
    controller ! ControlInvocation(AsyncRPCClient.IgnoreReply, StartWorkflow())
    parent.expectMsg(ControllerState.Running)
    parent.expectMsg(1.minute, ControllerState.Completed)
    parent.ref ! PoisonPill
    results
  }

  def executeWorkflowAsync(id: WorkflowIdentity, workflow: Workflow)(implicit
      actorSystem: ActorSystem
  ): (TestProbe, Future[Map[String, List[ITuple]]], ActorRef) = {
    val parent = TestProbe()
    val results: Promise[Map[String, List[ITuple]]] = new Promise()
    val eventListener = ControllerEventListener()
    eventListener.workflowCompletedListener = evt => results.setValue(evt.result)
    val controller = parent.childActorOf(
      Controller.props(id, workflow, eventListener, 100)
    )
    parent.expectMsg(ControllerState.Ready)
    controller ! ControlInvocation(AsyncRPCClient.IgnoreReply, StartWorkflow())
    parent.expectMsg(ControllerState.Running)
    (parent, results, controller)
  }

  def getControllerProps(
      operators: mutable.MutableList[OperatorDescriptor],
      links: mutable.MutableList[OperatorLink],
      jobId: String = "workflow-test",
      workflowTag: String = "workflow-test"
  ): Props = {
    val context = new WorkflowContext
    context.jobID = jobId

    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(operators, links, mutable.MutableList[BreakpointInfo]()),
      context
    )

    Controller.props(
      WorkflowIdentity(workflowTag),
      texeraWorkflowCompiler.amberWorkflow,
      ControllerEventListener(),
      100
    )
  }

}
