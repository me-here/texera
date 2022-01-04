package edu.uci.ics.texera.web

import rx.lang.scala.Observer

import scala.collection.mutable

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
