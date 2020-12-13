package edu.uci.ics.amber.engine.architecture.worker.neo

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import edu.uci.ics.amber.engine.architecture.worker.neo.PauseManager.PauseLevel._

object PauseManager {
  // I want to introduce pause privileges so that stronger pause can override weak pause
  // suppose:
  // 1. an internal control pauses the workflow (with privilege 1)
  // 2. user pauses the workflow (with privilege 2)
  // 3. the internal control resumes the workflow (with privilege 1)
  // step 3 will not be done since the user pauses the workflow.
  object PauseLevel {
    final val NoPause = 0
    final val System = 1
    final val User = 2
  }
}

class PauseManager {

  // current pause privilege level
  private val pausePrivilegeLevel = new AtomicInteger(NoPause)
  // yielded control of the dp thread
  // volatile is necessary otherwise main thread cannot notice the change.
  // volatile means read/writes are through memory rather than CPU cache
  @volatile
  private var currentFuture: CompletableFuture[Void] = _

  /** pause functionality
    * both dp thread and actor can call this function
    * @param level
    */
  def pause(level: Int): Boolean = {

    /*this line atomically applies the following logic:
      Level = Paused
      if(level >= pausePrivilegeLevel.get())
        pausePrivilegeLevel.set(level)
     */
    pausePrivilegeLevel.updateAndGet(i => if (level >= i) level else i) == level
  }

  /** blocking wait for dp thread to pause
    * MUST be called in worker actor thread
    */
  def waitForDPThread(): Unit = {
    while (currentFuture == null) {
      //wait
    }
  }

  /** resume functionality
    * only actor calls this function for now
    * @param level
    */
  def resume(level: Int): Boolean = {
    if (pausePrivilegeLevel.get() == NoPause) {
      return true
    }
    // only privilege level >= current pause privilege level can resume the worker
    if (pausePrivilegeLevel.updateAndGet(i => if (level >= i) NoPause else i) == NoPause) {
      unblockDPThread()
      true
    } else {
      false
    }
  }

  /** check for pause in dp thread
    * only dp thread and operator logic can call this function
    * @throws
    */
  @throws[Exception]
  def checkForPause(): Unit = {
    // returns if not paused
    if (this.pausePrivilegeLevel.get() == NoPause) return
    blockDPThread()
  }

  /** block the thread by creating CompletableFuture and wait for completion
    */
  private[this] def blockDPThread(): Unit = {
    // create a future and wait for its completion
    this.currentFuture = new CompletableFuture[Void]
    // thread blocks here
    this.currentFuture.get
  }

  /** unblock DP thread by resolving the CompletableFuture
    */
  private[this] def unblockDPThread(): Unit = {
    // If dp thread suspended, release it
    if (this.currentFuture != null) {
      this.currentFuture.complete(null)
      this.currentFuture = null
    }
  }

}
