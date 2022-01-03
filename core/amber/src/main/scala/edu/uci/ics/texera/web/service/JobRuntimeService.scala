package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.{ConditionalGlobalBreakpoint, CountGlobalBreakpoint}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent._
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.AssignBreakpointHandler.AssignGlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.EvaluatePythonExpressionHandler.EvaluatePythonExpression
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ResumeHandler.ResumeWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RetryWorkflowHandler.RetryWorkflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.model.websocket.event.python.PythonPrintTriggeredEvent
import edu.uci.ics.texera.web.{ObserverManager, SyncableState}
import edu.uci.ics.texera.web.model.websocket.event.{BreakpointTriggeredEvent, OperatorStatistics, OperatorStatisticsUpdateEvent, TexeraWebSocketEvent, WorkflowExecutionErrorEvent, WorkflowStateEvent}
import edu.uci.ics.texera.web.model.websocket.request.python.PythonExpressionEvaluateRequest
import edu.uci.ics.texera.web.model.websocket.request.{RemoveBreakpointRequest, SkipTupleRequest}
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource
import edu.uci.ics.texera.web.service.JobRuntimeService.bufferSize
import edu.uci.ics.texera.web.workflowruntimestate.BreakpointEvent.BreakpointTuple
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState._
import edu.uci.ics.texera.web.workflowruntimestate.{BreakpointEvent, OperatorRuntimeInfo, WorkflowAggregatedState, WorkflowJobRuntimeInfo}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.workflow.{Breakpoint, BreakpointCondition, ConditionBreakpoint, CountBreakpoint}
import rx.lang.scala.Observable
import rx.lang.scala.subjects.BehaviorSubject

import scala.collection.mutable

object JobRuntimeService {
  val bufferSize: Int = AmberUtils.amberConfig.getInt("web-server.python-console-buffer-size")

}

class JobRuntimeService(client: AmberClient, val subscriptionManager: ObserverManager[TexeraWebSocketEvent])
    extends SyncableState[WorkflowJobRuntimeInfo, TexeraWebSocketEvent]
    with LazyLogging {
  private val jobStatusObservable: BehaviorSubject[WorkflowAggregatedState] = BehaviorSubject[WorkflowAggregatedState](UNINITIALIZED)

  registerCallbacks()

  private[this] def registerCallbacks(): Unit = {
    registerCallbackOnBreakpoint()
    registerCallbackOnFatalError()
    registerCallbackOnPythonPrint()
    registerCallbackOnWorkflowComplete()
    registerCallbackOnWorkflowStatusUpdate()
  }

  /** *
    *  Callback Functions to register upon construction
    */
  private[this] def registerCallbackOnBreakpoint(): Unit = {
    client
      .getObservable[BreakpointTriggered]
      .subscribe((evt: BreakpointTriggered) => {
        modifyState{
          jobInfo =>
            val breakpointEvts = evt.report.filter(_._1._2 != null).map {
              elem =>
                val actorPath = elem._1._1.toString
                val faultedTuple = elem._1._2
                val tupleList =
                  if (faultedTuple.tuple != null) {
                    faultedTuple.tuple.toArray().filter(v => v != null).map(v => v.toString).toList
                  } else {
                    List.empty
                  }
                BreakpointEvent(actorPath, Some(BreakpointTuple(faultedTuple.id, faultedTuple.isInput, tupleList)),elem._2)
            }.toArray
            val newInfo = jobInfo.operatorInfo.getOrElse(evt.operatorID, OperatorRuntimeInfo()).withUnresolvedBreakpoints(breakpointEvts)
            jobInfo.addOperatorInfo((evt.operatorID, newInfo)).withState(PAUSED)
        }
      })
  }

  private[this] def registerCallbackOnWorkflowStatusUpdate(): Unit = {
    client
      .getObservable[WorkflowStatusUpdate]
      .subscribe((evt: WorkflowStatusUpdate) => {
        modifyState{
          jobInfo =>
            val updatedInfos = evt.operatorStatistics.map {
              case (opId, statistics) =>
                (opId, jobInfo.operatorInfo.getOrElse(opId, OperatorRuntimeInfo()).withStats(statistics))
            }
            jobInfo.addAllOperatorInfo(updatedInfos)
        }
      })
  }

  private[this] def registerCallbackOnWorkflowComplete(): Unit = {
    client
      .getObservable[WorkflowCompleted]
      .subscribe((evt: WorkflowCompleted) => {
        client.shutdown()
        modifyState(jobInfo => jobInfo.withState(COMPLETED))
      })
  }

  private[this] def registerCallbackOnPythonPrint(): Unit = {
    client
      .getObservable[PythonPrintTriggered]
      .subscribe((evt: PythonPrintTriggered) => {
        modifyState{
          jobInfo =>
            val opInfo = jobInfo.operatorInfo.getOrElse(evt.operatorID, OperatorRuntimeInfo())
            if(opInfo.pythonConsoleMessages.size < bufferSize){
              jobInfo.addOperatorInfo((evt.operatorID, opInfo.addPythonConsoleMessages(evt.message)))
            }else{
              jobInfo.addOperatorInfo((evt.operatorID, opInfo.withPythonConsoleMessages(opInfo.pythonConsoleMessages.drop(1):+evt.message)))
            }
        }
      })
  }

  private[this] def registerCallbackOnFatalError(): Unit = {
    client
      .getObservable[FatalError]
      .subscribe((evt: FatalError) => {
        client.shutdown()
        modifyState{
          jobInfo =>
            jobInfo.withState(ABORTED).withError(evt.e.getLocalizedMessage)
        }
      })
  }

  /** *
    *  Utility Functions
    */

  def getJobStatusObservable: Observable[WorkflowAggregatedState] = jobStatusObservable.onTerminateDetach

  def startWorkflow(): Unit = {
    val f = client.sendAsync(StartWorkflow())
    modifyState(jobInfo => jobInfo.withState(READY))
    f.onSuccess { _ =>
      modifyState(jobInfo => jobInfo.withState(RUNNING))
    }
  }

  def clearTriggeredBreakpoints(): Unit = {
    modifyState{
      jobInfo =>
        jobInfo.withOperatorInfo(jobInfo.operatorInfo.map(pair => pair._1 -> pair._2.withUnresolvedBreakpoints(Seq.empty)))
    }
  }

  def addBreakpoint(operatorID: String, breakpoint: Breakpoint): Unit = {
    val breakpointID = "breakpoint-" + operatorID + "-" + System.currentTimeMillis()
    breakpoint match {
      case conditionBp: ConditionBreakpoint =>
        val column = conditionBp.column
        val predicate: Tuple => Boolean = conditionBp.condition match {
          case BreakpointCondition.EQ =>
            tuple => {
              tuple.getField(column).toString.trim == conditionBp.value
            }
          case BreakpointCondition.LT =>
            tuple => tuple.getField(column).toString.trim < conditionBp.value
          case BreakpointCondition.LE =>
            tuple => tuple.getField(column).toString.trim <= conditionBp.value
          case BreakpointCondition.GT =>
            tuple => tuple.getField(column).toString.trim > conditionBp.value
          case BreakpointCondition.GE =>
            tuple => tuple.getField(column).toString.trim >= conditionBp.value
          case BreakpointCondition.NE =>
            tuple => tuple.getField(column).toString.trim != conditionBp.value
          case BreakpointCondition.CONTAINS =>
            tuple => tuple.getField(column).toString.trim.contains(conditionBp.value)
          case BreakpointCondition.NOT_CONTAINS =>
            tuple => !tuple.getField(column).toString.trim.contains(conditionBp.value)
        }

        client.sendSync(
          AssignGlobalBreakpoint(
            new ConditionalGlobalBreakpoint(
              breakpointID,
              tuple => {
                val texeraTuple = tuple.asInstanceOf[Tuple]
                predicate.apply(texeraTuple)
              }
            ),
            operatorID
          )
        )
      case countBp: CountBreakpoint =>
        client.sendSync(
          AssignGlobalBreakpoint(new CountGlobalBreakpoint(breakpointID, countBp.count), operatorID)
        )
    }
  }

  def skipTuple(tupleReq: SkipTupleRequest): Unit = {
    throw new RuntimeException("skipping tuple is temporarily disabled")
  }

  def modifyLogic(operatorDescriptor: OperatorDescriptor): Unit = {
    client.sendAsync(ModifyLogic(operatorDescriptor))
  }

  def retryWorkflow(): Unit = {
    clearTriggeredBreakpoints()
    val f = client.sendAsync(RetryWorkflow())
    modifyState(jobInfo => jobInfo.withState(RESUMING))
    f.onSuccess { _ =>
      modifyState(jobInfo => jobInfo.withState(RUNNING))
    }
  }

  def pauseWorkflow(): Unit = {
    val f = client.sendAsync(PauseWorkflow())
    modifyState(jobInfo => jobInfo.withState(PAUSING))
    f.onSuccess { _ =>
      modifyState(jobInfo => jobInfo.withState(PAUSED))
    }
  }

  def resumeWorkflow(): Unit = {
    clearTriggeredBreakpoints()
    val f = client.sendAsync(ResumeWorkflow())
    modifyState(jobInfo => jobInfo.withState(RESUMING))
    f.onSuccess { _ =>
      modifyState(jobInfo => jobInfo.withState(RUNNING))
    }
  }

  def killWorkflow(): Unit = {
    client.shutdown()
    modifyState(jobInfo => jobInfo.withState(ABORTED))
  }

  def removeBreakpoint(removeBreakpoint: RemoveBreakpointRequest): Unit = {
    throw new UnsupportedOperationException()
  }

  def evaluatePythonExpression(request: PythonExpressionEvaluateRequest): Unit = {
    client.sendSync(EvaluatePythonExpression(request.expression, request.operatorId))
  }


  override def computeDiff(oldState: WorkflowJobRuntimeInfo, newState: WorkflowJobRuntimeInfo): Array[TexeraWebSocketEvent] = {
    val buf = mutable.ArrayBuffer[TexeraWebSocketEvent]()
    // Update operator stats if any operator updates its stat
    if(newState.operatorInfo.map(_._2.stats).toSet != oldState.operatorInfo.map(_._2.stats).toSet){
      buf += OperatorStatisticsUpdateEvent(newState.operatorInfo.collect{
        case x if x._2.stats.isDefined =>
          val stats = x._2.stats.get
          val res = OperatorStatistics(Utils.aggregatedStateToString(stats.state),stats.inputCount, stats.outputCount)
          (x._1,res)
      })
    }
    // Update workflow state
    if(newState.state != oldState.state){
      jobStatusObservable.onNext(newState.state)
      buf += WorkflowStateEvent(Utils.aggregatedStateToString(newState.state))
    }
    // For each operator, check if it has new python console message or breakpoint events
    newState.operatorInfo.foreach {
      case (opId, info) =>
        val oldInfo = oldState.operatorInfo.getOrElse(opId, new OperatorRuntimeInfo())
        if (info.unresolvedBreakpoints.nonEmpty && info.unresolvedBreakpoints != oldInfo.unresolvedBreakpoints) {
          buf += BreakpointTriggeredEvent(info.unresolvedBreakpoints, opId)
        }
        if (info.pythonConsoleMessages.nonEmpty && info.pythonConsoleMessages != oldInfo.pythonConsoleMessages) {
          val stringBuilder = new StringBuilder()
          info.pythonConsoleMessages.foreach(s => stringBuilder.append(s))
          buf += PythonPrintTriggeredEvent(stringBuilder.toString(), opId)
        }
    }
    // Check if new error occurred
    if(newState.error != oldState.error){
      buf += WorkflowExecutionErrorEvent(newState.error)
    }
    buf.toArray
  }

  override def defaultState: WorkflowJobRuntimeInfo = WorkflowJobRuntimeInfo()
}
