package edu.uci.ics.texera.web

import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.workflowcachestate.WorkflowCacheStore
import edu.uci.ics.texera.web.workflowresultstate.WorkflowResultStore
import edu.uci.ics.texera.web.workflowruntimestate.{
  JobBreakpointStore,
  JobPythonStore,
  JobStateStore,
  JobStatsStore
}
import rx.lang.scala.{Observable, Observer, Subject, Subscription}

class WorkflowStateStore(wsOutput: WebsocketOutput) {
  val cacheStore = new SyncableState(WorkflowCacheStore())
  val statsStore = new SyncableState(JobStatsStore())
  val jobStateStore = new SyncableState(JobStateStore())
  val pythonStore = new SyncableState(JobPythonStore())
  val breakpointStore = new SyncableState(JobBreakpointStore())
  val resultStore = new SyncableState(WorkflowResultStore())

  def getAllStores: Iterable[SyncableState[_]] = {
    Iterable(cacheStore, statsStore, pythonStore, breakpointStore, resultStore, jobStateStore)
  }

  def fetchSnapshotThenSendTo(ob: Observer[TexeraWebSocketEvent]): Subscription = {
    val allStores = getAllStores
    // acquire all locks
    allStores.foreach(_.acquireLock())

    wsOutput.withUnicastMode(ob) {
      // send snapshot with unicast mode
      allStores.foreach(_.sendSnapshot())
    }

    // add observer to ws output subject
    val subscription = wsOutput.subscribe(ob)

    // release all locks
    allStores.foreach(_.releaseLock())

    subscription
  }
}
