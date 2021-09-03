package edu.uci.ics.texera.web

import com.typesafe.config.ConfigFactory
import edu.uci.ics.amber.engine.common.AmberUtils
import kamon.Kamon

object TexeraRunWorker {

  def main(args: Array[String]): Unit = {
    Kamon.init()
    // start actor system worker node
    AmberUtils.startActorWorker(Option.empty)
  }

}
