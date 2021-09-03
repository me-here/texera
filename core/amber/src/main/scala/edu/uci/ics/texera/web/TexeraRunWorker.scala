package edu.uci.ics.texera.web

import com.typesafe.config.ConfigFactory
import edu.uci.ics.amber.engine.common.AmberUtils
import kamon.Kamon

object TexeraRunWorker {

  def main(args: Array[String]): Unit = {
    Kamon.init()
    Kamon.reconfigure(ConfigFactory.parseString("kamon.metric.tick-interval=10s").withFallback(Kamon.config()))
    // start actor system worker node
    AmberUtils.startActorWorker(Option.empty)
  }

}
