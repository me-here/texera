package edu.uci.ics.texera.web

import com.typesafe.config.{Config, ConfigFactory}
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.AmberUtils.akkaConfig
import kamon.Kamon

object TexeraRunWorker {
  def kamonConfig: Config = ConfigFactory.load("kamon").withFallback(akkaConfig)
  def main(args: Array[String]): Unit = {

    Kamon.init(kamonConfig)

    // start actor system worker node
    AmberUtils.startActorWorker(Option.empty)
  }

}
