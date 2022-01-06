package edu.uci.ics.texera.web

import rx.lang.scala.Subscription

import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

class SyncableState[T](defaultStateGen: => T) {

  private var state: T = defaultStateGen
  private var isModifying = false
  private var callbackId = 0
  private var onChangedCallbacks = mutable.HashMap[Int, (T, T) => Unit]()
  private val lock = new ReentrantLock()

  def getStateThenConsume[X](next: T => X): X = {
    withLock {
      next(state)
    }
  }

  def acquireLock(): Unit = lock.lock()
  def releaseLock(): Unit = lock.unlock()

  private def withLock[X](code: => X): X = {
    acquireLock()
    val result = code
    releaseLock()
    result
  }

  def updateState(func: T => T): Unit = {
    withLock {
      assert(!isModifying, "Cannot recursively update state or update state inside onChanged")
      isModifying = true
      val newState = func(state)
      onChangedCallbacks.values.foreach(callback => callback(state, newState))
      isModifying = false
      state = newState
    }
  }

  def onChanged(callback: (T, T) => Unit): Subscription = {
    withLock {
      onChangedCallbacks(callbackId) = callback
      val id = callbackId
      val sub = Subscription {
        onChangedCallbacks.remove(id)
      }
      callbackId += 1
      sub
    }
  }

  def sendSnapshot(): Unit = {
    withLock {
      onChangedCallbacks.values.foreach(callback => callback(defaultStateGen, state))
    }
  }

}
