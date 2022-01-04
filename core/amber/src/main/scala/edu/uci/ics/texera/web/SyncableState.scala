package edu.uci.ics.texera.web

abstract class SyncableState[T, U] {

  private var state: T = defaultState
  private var isModifying = false

  def defaultState: T

  def getStateThenConsume[X](next: T => X): X = next(state)

  def modifyState(func: T => T): Unit = {
    synchronized {
      assert(!isModifying, "Cannot recursively modify state or modify state inside computeDiff")
      isModifying = true
      val newState = func(state)
      computeDiff(state, newState).foreach(delta => subscriptionManager.pushToObservers(delta))
      isModifying = false
      state = newState
    }
  }

  def subscriptionManager: ObserverManager[U]

  def computeDiff(oldState: T, newState: T): Array[U]

  // To make sure the state is accessed synchronously
  def computeSnapshotThenConsume(next: Array[U] => Unit): Unit = {
    synchronized {
      next(computeDiff(defaultState, state))
    }
  }

}
