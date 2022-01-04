package edu.uci.ics.texera.web.service

import java.util.concurrent.ConcurrentHashMap
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.{ObserverManager, TexeraWebApplication, WorkflowLifecycleManager}
import edu.uci.ics.texera.web.model.websocket.request.WorkflowExecuteRequest
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import org.jooq.types.UInteger
import rx.lang.scala.subjects.BehaviorSubject
import rx.lang.scala.{Observable, Observer}

object WorkflowService {
  private val wIdToWorkflowState = new ConcurrentHashMap[String, WorkflowService]()
  val cleanUpDeadlineInSeconds: Int =
    AmberUtils.amberConfig.getInt("web-server.workflow-state-cleanup-in-seconds")
  def getOrCreate(wId: String, cleanupTimeout: Int = cleanUpDeadlineInSeconds): WorkflowService = {
    wIdToWorkflowState.compute(
      wId,
      (_, v) => {
        if (v == null) {
          new WorkflowService(wId, cleanupTimeout)
        } else {
          v
        }
      }
    )
  }
}

class WorkflowService(wid: String, cleanUpTimeout: Int) extends LazyLogging {
  // state across execution:
  var opResultStorage: OpResultStorage = new OpResultStorage(
    AmberUtils.amberConfig.getString("storage.mode").toLowerCase
  )
  val userSessionSubscriptionManager = new ObserverManager[TexeraWebSocketEvent]()
  val resultService: JobResultService =
    new JobResultService(opResultStorage, userSessionSubscriptionManager)
  val exportService: ResultExportService = new ResultExportService(opResultStorage)
  val operatorCache: WorkflowCacheService =
    new WorkflowCacheService(opResultStorage, userSessionSubscriptionManager)
  var jobService: Option[WorkflowJobService] = None
  val lifeCycleManager: WorkflowLifecycleManager = new WorkflowLifecycleManager(
    wid,
    cleanUpTimeout,
    () => {
      WorkflowService.wIdToWorkflowState.remove(wid)
      jobService.foreach(_.workflowRuntimeService.killWorkflow())
    }
  )
  private val jobStateSubject = BehaviorSubject[WorkflowJobService]()

  def connect(observer: Observer[TexeraWebSocketEvent]): Unit = {
    resultService.computeSnapshotThenConsume(events => {
      events.foreach(evt => observer.onNext(evt))
      userSessionSubscriptionManager.addObserver(observer)
    })
    lifeCycleManager.increaseUserCount()
  }

  def disconnect(observer: Observer[TexeraWebSocketEvent]): Unit = {
    userSessionSubscriptionManager.removeObserver(observer)
    lifeCycleManager.decreaseUserCount(
      jobService.map(_.workflowRuntimeService.getStateThenConsume(_.state))
    )
  }

  def initJobService(req: WorkflowExecuteRequest, uidOpt: Option[UInteger]): Unit = {
    val job = new WorkflowJobService(
      userSessionSubscriptionManager,
      operatorCache,
      resultService,
      uidOpt,
      req
    )
    lifeCycleManager.connectJobService(job)
    jobService = Some(job)
    jobStateSubject.onNext(job)
    job.startWorkflow()
  }

  def getJobServiceObservable: Observable[WorkflowJobService] = jobStateSubject.onTerminateDetach
}
