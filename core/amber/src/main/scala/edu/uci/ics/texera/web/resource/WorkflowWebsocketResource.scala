package edu.uci.ics.texera.web.resource

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{ActorRef, PoisonPill}
import edu.uci.ics.amber.engine.architecture.controller.{Controller, ControllerEventListener}
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage._
import edu.uci.ics.amber.engine.common.ambermessage.ControllerMessage.AckedControllerInitialization
import edu.uci.ics.amber.engine.common.ambertag.WorkflowTag
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.web.TexeraWebApplication
import edu.uci.ics.texera.web.model.event._
import edu.uci.ics.texera.web.model.request._
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource.{
  getDirtyPageIndices,
  sessionResults
}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.{WorkflowCompiler, WorkflowInfo}
import edu.uci.ics.texera.workflow.common.{Utils, WorkflowContext}

import javax.websocket._
import javax.websocket.server.ServerEndpoint
import scala.collection.mutable

object WorkflowWebsocketResource {

  val nextWorkflowID = new AtomicInteger(0)

  val sessionMap = new mutable.HashMap[String, Session]
  val sessionJobs = new mutable.HashMap[String, (WorkflowCompiler, ActorRef)]
  val sessionResults = new mutable.HashMap[String, Map[String, List[ITuple]]]

  /**
    * Calculate which page in frontend need to be re-fetched
    * @param beforeList data before status update event (i.e. unmodified sessionResults)
    * @param afterList data after status update event
    * @return list of indices of modified pages starting from 1
    */
  def getDirtyPageIndices(beforeList: List[ITuple], afterList: List[ITuple]): List[Int] = {
    val pageSize = 10

    var currentIndex = 1
    var currentIndexPageCount = 0
    val dirtyPageIndices = new mutable.HashSet[Int]()
    for ((before, after) <- beforeList.zipAll(afterList, null, null)) {
      if (before == null || after == null || !before.equals(after)) {
        dirtyPageIndices.add(currentIndex)
      }
      currentIndexPageCount += 1
      if (currentIndexPageCount == pageSize) {
        currentIndexPageCount = 0
        currentIndex += 1
      }
    }

    dirtyPageIndices.toList
  }
}

@ServerEndpoint("/wsapi/workflow-websocket")
class WorkflowWebsocketResource {

  final val objectMapper = Utils.objectMapper

  @OnOpen
  def myOnOpen(session: Session): Unit = {
    WorkflowWebsocketResource.sessionMap.update(session.getId, session)
    println("connection open")
  }

  @OnMessage
  def myOnMsg(session: Session, message: String): Unit = {
    val request = objectMapper.readValue(message, classOf[TexeraWebSocketRequest])
    println(request)
    try {
      request match {
        case helloWorld: HelloWorldRequest =>
          send(session, HelloWorldResponse("hello from texera web server"))
        case execute: ExecuteWorkflowRequest =>
          println(execute)
          executeWorkflow(session, execute)
        case newLogic: ModifyLogicRequest =>
          println(newLogic)
          modifyLogic(session, newLogic)
        case pause: PauseWorkflowRequest =>
          pauseWorkflow(session)
        case resume: ResumeWorkflowRequest =>
          resumeWorkflow(session)
        case kill: KillWorkflowRequest =>
          killWorkflow(session)
        case skipTupleMsg: SkipTupleRequest =>
          skipTuple(session, skipTupleMsg)
        case breakpoint: AddBreakpointRequest =>
          addBreakpoint(session, breakpoint)
        case paginationRequest: ResultPaginationRequest =>
          resultPagination(session, paginationRequest)
      }
    } catch {
      case e: Throwable => {
        send(session, WorkflowErrorEvent(generalErrors = Map("exception" -> e.getMessage)))
        throw e
      }
    }

  }

  @OnClose
  def myOnClose(session: Session, cr: CloseReason): Unit = {
    if (WorkflowWebsocketResource.sessionJobs.contains(session.getId)) {
      println(s"session ${session.getId} disconnected, kill its controller actor")
      this.killWorkflow(session)
    }

    sessionResults.remove(session.getId)
  }

  def send(session: Session, event: TexeraWebSocketEvent): Unit = {
    session.getAsyncRemote.sendText(objectMapper.writeValueAsString(event))
  }

  def resultPagination(session: Session, request: ResultPaginationRequest): Unit = {
    val paginatedResultEvent = PaginatedResultEvent(
      sessionResults(session.getId)
        .map {
          case (operatorID, table) =>
            (
              operatorID,
              table
                .slice(
                  request.pageSize * (request.pageIndex - 1),
                  request.pageSize * request.pageIndex
                )
                .map(tuple => tuple.asInstanceOf[Tuple].asKeyValuePairJson())
            )
        }
        .map {
          case (operatorID, objNodes) =>
            PaginatedOperatorResult(
              operatorID,
              objNodes,
              sessionResults(session.getId)(operatorID).size
            )
        }
        .toList
    )

    send(session, paginatedResultEvent)
  }

  def addBreakpoint(session: Session, addBreakpoint: AddBreakpointRequest): Unit = {
    val compiler = WorkflowWebsocketResource.sessionJobs(session.getId)._1
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    compiler.addBreakpoint(controller, addBreakpoint.operatorID, addBreakpoint.breakpoint)
  }

  def removeBreakpoint(session: Session, removeBreakpoint: RemoveBreakpointRequest): Unit = {
    throw new UnsupportedOperationException();
  }

  def skipTuple(session: Session, tupleReq: SkipTupleRequest): Unit = {
    val actorPath = tupleReq.actorPath
    val faultedTuple = tupleReq.faultedTuple
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    controller ! SkipTupleGivenWorkerRef(actorPath, faultedTuple.toFaultedTuple())
  }

  def modifyLogic(session: Session, newLogic: ModifyLogicRequest): Unit = {
//    val texeraOperator = newLogic.operator
//    val (compiler, controller) = WorkflowWebsocketResource.sessionJobs(session.getId)
//    compiler.initOperator(texeraOperator)
//    controller ! ModifyLogic(texeraOperator.operatorExecutor)
    throw new RuntimeException("modify logic is temporarily disabled")
  }

  def pauseWorkflow(session: Session): Unit = {
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    controller ! Pause
    // workflow paused event will be send after workflow is actually paused
    // the callback function will handle sending the paused event to frontend
  }

  def resumeWorkflow(session: Session): Unit = {
    val controller = WorkflowWebsocketResource.sessionJobs(session.getId)._2
    controller ! Resume
    send(session, WorkflowResumedEvent())
  }

  def killWorkflow(session: Session): Unit = {
    WorkflowWebsocketResource.sessionJobs(session.getId)._2 ! PoisonPill
    println("workflow killed")
  }

  def executeWorkflow(session: Session, request: ExecuteWorkflowRequest): Unit = {
    val context = new WorkflowContext
    val workflowID = Integer.toString(WorkflowWebsocketResource.nextWorkflowID.incrementAndGet)
    context.workflowID = workflowID

    val texeraWorkflowCompiler = new WorkflowCompiler(
      WorkflowInfo(request.operators, request.links, request.breakpoints),
      context
    )

    texeraWorkflowCompiler.init()
    val violations = texeraWorkflowCompiler.validate
    if (violations.nonEmpty) {
      send(session, WorkflowErrorEvent(violations))
      return
    }

    val workflow = texeraWorkflowCompiler.amberWorkflow
    val workflowTag = WorkflowTag.apply(workflowID)

    val eventListener = ControllerEventListener(
      workflowCompletedListener = completed => {
        sessionResults.update(session.getId, completed.result)
        send(session, WorkflowCompletedEvent.apply(completed, texeraWorkflowCompiler))
        WorkflowWebsocketResource.sessionJobs.remove(session.getId)
      },
      workflowStatusUpdateListener = statusUpdate => {

        val sinkOpDirtyPageIndices = statusUpdate.operatorStatistics
          .filter(e => e._2.aggregatedOutputResults.isDefined)
          .map(e => {
            val beforeList =
              sessionResults.getOrElse(session.getId, Map.empty).getOrElse(e._1, List.empty)
            val afterList = e._2.aggregatedOutputResults.get
            val dirtyPageIndices = getDirtyPageIndices(beforeList, afterList)
            (e._1, dirtyPageIndices)
          })

        sessionResults.update(
          session.getId,
          statusUpdate.operatorStatistics
            .filter(e => e._2.aggregatedOutputResults.isDefined)
            .map(e => (e._1, e._2.aggregatedOutputResults.get))
        )
        send(
          session,
          WorkflowStatusUpdateEvent.apply(
            statusUpdate.operatorStatistics,
            sinkOpDirtyPageIndices,
            texeraWorkflowCompiler
          )
        )
      },
      modifyLogicCompletedListener = _ => {
        send(session, ModifyLogicCompletedEvent())
      },
      breakpointTriggeredListener = breakpointTriggered => {
        send(session, BreakpointTriggeredEvent.apply(breakpointTriggered))
      },
      workflowPausedListener = _ => {
        send(session, WorkflowPausedEvent())
      },
      skipTupleResponseListener = _ => {
        send(session, SkipTupleResponseEvent())
      },
      reportCurrentTuplesListener = report => {
//        send(session, OperatorCurrentTuplesUpdateEvent.apply(report))
      },
      recoveryStartedListener = _ => {
        send(session, RecoveryStartedEvent())
      },
      workflowExecutionErrorListener = errorOccurred => {
        send(session, WorkflowExecutionErrorEvent(errorOccurred.error.convertToMap()))
      }
    )

    val controllerActorRef = TexeraWebApplication.actorSystem.actorOf(
      Controller.props(workflowTag, workflow, false, eventListener, 1000)
    )
    controllerActorRef ! AckedControllerInitialization
    texeraWorkflowCompiler.initializeBreakpoint(controllerActorRef)
    controllerActorRef ! Start

    WorkflowWebsocketResource.sessionJobs(session.getId) =
      (texeraWorkflowCompiler, controllerActorRef)

    send(session, WorkflowStartedEvent())

  }

}
