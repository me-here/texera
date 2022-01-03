package edu.uci.ics.texera.web.service

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, Workflow}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.WorkflowIdentity
import edu.uci.ics.texera.web.{ObserverManager, TexeraWebApplication}
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.model.websocket.request.{CacheStatusUpdateRequest, ModifyLogicRequest, ResultExportRequest, WorkflowExecuteRequest}
import edu.uci.ics.texera.web.model.websocket.response.ResultExportResponse
import edu.uci.ics.texera.web.resource.WorkflowWebsocketResource
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler.ConstraintViolationException
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo.toJgraphtDAG
import edu.uci.ics.texera.workflow.common.workflow.{WorkflowCompiler, WorkflowInfo, WorkflowRewriter}
import org.jooq.types.UInteger

class WorkflowJobService(
                          subscriptionManager: ObserverManager[TexeraWebSocketEvent],
    operatorCache: WorkflowCacheService,
    resultService: JobResultService,
    uidOpt: Option[UInteger],
    request: WorkflowExecuteRequest
) extends LazyLogging {

  // Compilation starts from here:
  val workflowContext: WorkflowContext = createWorkflowContext()
  val workflowInfo: WorkflowInfo = createWorkflowInfo(workflowContext)
  val workflowCompiler: WorkflowCompiler = createWorkflowCompiler(workflowInfo, workflowContext)
  val workflow: Workflow = workflowCompiler.amberWorkflow(
    WorkflowIdentity(workflowContext.jobId),
    resultService.opResultStorage
  )

  // Runtime starts from here:
  val client: AmberClient =
    TexeraWebApplication.createAmberRuntime(workflow, ControllerConfig.default)
  val workflowRuntimeService: JobRuntimeService = new JobRuntimeService(client, subscriptionManager)

  def startWorkflow(): Unit = {
    for (pair <- workflowInfo.breakpoints) {
      workflowRuntimeService.addBreakpoint(pair.operatorID, pair.breakpoint)
    }
    resultService.attachToJob(workflowInfo, client)
    workflowRuntimeService.startWorkflow()
  }

  private[this] def createWorkflowContext(): WorkflowContext = {
    val jobID: String = Integer.toString(WorkflowWebsocketResource.nextExecutionID.incrementAndGet)
    if (WorkflowCacheService.isAvailable) {
      operatorCache.updateCacheStatus(
        CacheStatusUpdateRequest(
          request.operators,
          request.links,
          request.breakpoints,
          request.cachedOperatorIds
        )
      )
    }
    val context = new WorkflowContext
    context.jobId = jobID
    context.userId = uidOpt
    context
  }

  private[this] def createWorkflowInfo(context: WorkflowContext): WorkflowInfo = {
    var workflowInfo = WorkflowInfo(request.operators, request.links, request.breakpoints)
    if (WorkflowCacheService.isAvailable) {
      workflowInfo.cachedOperatorIds = request.cachedOperatorIds
      logger.debug(
        s"Cached operators: ${operatorCache.cachedOperators} with ${request.cachedOperatorIds}"
      )
      val workflowRewriter = new WorkflowRewriter(
        workflowInfo,
        operatorCache.cachedOperators,
        operatorCache.cacheSourceOperators,
        operatorCache.cacheSinkOperators,
        operatorCache.operatorRecord,
        resultService.opResultStorage
      )
      val newWorkflowInfo = workflowRewriter.rewrite
      val oldWorkflowInfo = workflowInfo
      workflowInfo = newWorkflowInfo
      workflowInfo.cachedOperatorIds = oldWorkflowInfo.cachedOperatorIds
      logger.info(
        s"Rewrite the original workflow: ${toJgraphtDAG(oldWorkflowInfo)} to be: ${toJgraphtDAG(workflowInfo)}"
      )
    }
    workflowInfo
  }

  private[this] def createWorkflowCompiler(
      workflowInfo: WorkflowInfo,
      context: WorkflowContext
  ): WorkflowCompiler = {
    val compiler = new WorkflowCompiler(workflowInfo, context)
    val violations = compiler.validate
    if (violations.nonEmpty) {
      throw new ConstraintViolationException(violations)
    }
    compiler
  }

  def modifyLogic(request: ModifyLogicRequest): Unit = {
    workflowCompiler.initOperator(request.operator)
    workflowRuntimeService.modifyLogic(request.operator)
  }

}
