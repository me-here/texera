package edu.uci.ics.texera.web

import rx.lang.scala.{Observer, Subscriber}

import scala.collection.mutable

abstract class SyncableState[T, U] {

  private var state: T = defaultState
  private var isModifying = false

  def defaultState: T

  def getState: T = state

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

  def computeSnapshot: Array[U] = {
    computeDiff(defaultState, state)
  }

}

class ObserverManager[U] {
  private val observers: mutable.ArrayBuffer[Observer[U]] = mutable.ArrayBuffer.empty

  def addObserver(ob: Observer[U]): Unit = {
    observers += ob
  }

  def removeObserver(ob: Observer[U]): Unit = {
    observers -= ob
  }

  def pushToObservers(msg: U): Unit = {
    observers.foreach(ob => ob.onNext(msg))
  }
}
