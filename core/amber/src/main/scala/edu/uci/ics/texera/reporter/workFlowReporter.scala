package edu.uci.ics.texera.reporter

import com.typesafe.config.Config
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.reporter.workFlowReporter.Configuration
import kamon.Kamon
import kamon.metric.MeasurementUnit.Dimension.{Information, Time}
import kamon.metric.MeasurementUnit.{information, time}
import kamon.metric._
import kamon.module.MetricReporter
import kamon.tag.{Lookups, Tag, TagSet}
import kamon.util.{EnvironmentTags, Filter}
import org.slf4j.LoggerFactory

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.time.Duration
import java.util.Locale
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.asScalaBufferConverter

class workFlowReporter(
                        @volatile private var configuration: Configuration,
                        var relateActorPath: String,
                        var relateWorkerIdentity: ActorVirtualIdentity
) extends MetricReporter {
  val expectedMetrics = configuration.metricList
  import workFlowReporter._

  private val logger = LoggerFactory.getLogger(classOf[workFlowReporter])
  private val symbols = DecimalFormatSymbols.getInstance(Locale.US)
  private val valueFormat = new DecimalFormat("#0.#########", symbols)
  var actorMetrics = new mutable.HashMap[String, ListBuffer[MetricWrapper]]
  symbols.setDecimalSeparator(
    '.'
  ) // Just in case there is some weird locale config we are not aware of.

  override def stop(): Unit = {
    logger.info(relateActorPath + " Stopped the workFlowWorker API reporter.")
    val metricList = actorMetrics.getOrElse(relateActorPath, null)

    if (metricList != null) {
      logger.debug(
        s" ActorId:$relateWorkerIdentity | ActorPath:$relateActorPath | Metric: | ${metricList.mkString("-------------------", "-------------------", "\n-------------------")}"
      )
    }
  }

  override def reconfigure(config: Config): Unit = {
    val newConfiguration = readConfiguration(config)
    configuration = newConfiguration
  }

  override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = {
    logger.info("reportPeriodSnapshot begin")
    addData(snapshot)

  }

  private def addData(snapshot: PeriodSnapshot): Unit = {
    val metricList = actorMetrics.getOrElse(relateActorPath, new ListBuffer[MetricWrapper])
    val timestampStart = snapshot.from.toEpochMilli.toString
    val timestampEnd = snapshot.to.toEpochMilli.toString
    val interval = Math.round(Duration.between(snapshot.from, snapshot.to).toMillis())

    def addDistribution(metric: MetricSnapshot.Distributions): Unit = {
      var metricValueMap = new mutable.HashMap[String, String]
      val unit = metric.settings.unit
      metric.instruments.filter(p => {
        pathFilter[Distribution](p)
      }) foreach { d =>
        val dist = d.value
        val average = if (dist.count > 0L) (dist.sum / dist.count) else 0L
        metricValueMap.put("avg", valueFormat.format(scale(average, unit)))
        metricValueMap.put("count", valueFormat.format(dist.count))
        metricValueMap.put("median", valueFormat.format(scale(dist.percentile(50d).value, unit)))
        metricValueMap.put("max", valueFormat.format(scale(dist.max, unit)))
        metricValueMap.put("min", valueFormat.format(scale(dist.min, unit)))
        val metricUnit = unitToString(unit)
        addMetric(
          metric.name,
          gauge,
          d.tags,
          metricValueMap,
          metricUnit
        )
      }
    }
    def addMetric(
        metricClass: String,
        metricType: String,
        tags: TagSet,
        metricValueMap: mutable.HashMap[String, String],
        metricValueUnit: String
    ): Unit = {

      val customTagsMap: mutable.Map[String, String] = mutable.HashMap[String, String]()
      (configuration.extraTags ++ tags
        .iterator(_.toString)
        .map(p => p.key -> p.value)
        .filter(t => configuration.tagFilter.accept(t._1))).foreach(e =>
        customTagsMap += ((e._1, e._2))
      )
      customTagsMap.put("metricValueUnit", metricValueUnit)
      customTagsMap.put("timeStampUnit", configuration.timeUnit.magnitude.name)
      var metricWrapper = new MetricWrapper(
        metricClass,
        interval.toString,
        timestampStart,
        timestampEnd,
        metricValueMap.toMap,
        metricType,
        customTagsMap.toMap
      )
      metricList.append(metricWrapper)

    }
    snapshot.counters
      .filter(dis => {
        expectedMetrics.contains(dis.name)
      })
      .foreach { snap =>
        snap.instruments.foreach { instrument =>
          {
            var metricValueMap = new mutable.HashMap[String, String]
            metricValueMap.put(
              "count",
              valueFormat.format(scale(instrument.value, snap.settings.unit))
            )
            addMetric(
              snap.name,
              count,
              instrument.tags,
              metricValueMap,
              "#"
            )
          }
        }
      }
    snapshot.gauges
      .filter(dis => {
        expectedMetrics.contains(dis.name)
      })
      .foreach { snap =>
        snap.instruments.foreach { instrument =>
          {
            var metricValueMap = new mutable.HashMap[String, String]
            metricValueMap.put(
              "count",
              valueFormat.format(scale(instrument.value, snap.settings.unit))
            )
            addMetric(
              snap.name,
              gauge,
              instrument.tags,
              metricValueMap,
              "#"
            )
          }
        }
      }

    (snapshot.histograms ++ snapshot.rangeSamplers ++ snapshot.timers)
      .filter(dis => {
        expectedMetrics.contains(dis.name)
      })
      .foreach(addDistribution)

    actorMetrics.put(relateActorPath, metricList)

  }

  private def pathFilter[T](instrument: Instrument.Snapshot[T]): Boolean = {
    val path = instrument.tags.get(Lookups.plain("path"))
    if (path != null) {
      if (path.contains(relateActorPath)) {
        true
      } else false
    } else false
  }

  private def scale(value: Double, unit: MeasurementUnit): Double =
    unit.dimension match {
      case Time if unit.magnitude != configuration.timeUnit.magnitude =>
        MeasurementUnit.convert(value, unit, configuration.timeUnit)

      case Information if unit.magnitude != configuration.informationUnit.magnitude =>
        MeasurementUnit.convert(value, unit, configuration.informationUnit)

      case _ => value
    }

  private def unitToString(unit: MeasurementUnit): String = {
    val unitString: String =
      unit.dimension match {
        case Time => { configuration.timeUnit.magnitude.name }

        case Information => configuration.informationUnit.magnitude.name

        case _ => "#"
      }
    if (unitString == null)
      "#"
    else unitString

  }

  case class MetricWrapper(
      var name: String,
      var interval: String,
      var startTimeStamp: String,
      var endTimeStamp: String,
      var metricValue: Map[String, String],
      var metricType: String,
      var tags: Map[String, String]
  ) {

    override def toString: String = {
      val allTagsString = tags.mkString("[", ",", "]")
      val v =
        """
          |%s
          |Time Interval:[%s,%s](Unit:%s)
          |Time : %s(Unit:%s)
          |Value:%s(Unit:%s)
          |MetricType:%s
          |Tags:%s
          |""".stripMargin.format(
          name,
          startTimeStamp,
          endTimeStamp,
          tags("timeStampUnit"),
          interval,
          tags("timeStampUnit"),
          metricValue,
          configuration.timeUnit.magnitude.name,
          metricType,
          allTagsString
        )
      v
    }

  }

  def getActorProcessingTime: ListBuffer[MetricWrapper] = {
    actorMetrics(relateActorPath).filter(w => { w.name == "akka.actor.processing-time" })
  }

  def getActorMailboxStats: ListBuffer[MetricWrapper] = {
    actorMetrics(relateActorPath).filter(w => {
      w.name == "akka.actor.time-in-mailbox" || w.name == "akka.actor.mailbox-size"
    })
  }

}

object workFlowReporter {
  val count = "count"
  val gauge = "gauge"

  def readConfiguration(config: Config): Configuration = {
    val reporterConfig = config.getConfig("kamon.workerFlowWorkerReporter")
    Configuration(
      timeUnit = readTimeUnit(reporterConfig.getString("time-unit")),
      informationUnit = readInformationUnit(reporterConfig.getString("information-unit")),
      EnvironmentTags
        .from(Kamon.environment, reporterConfig.getConfig("environment-tags"))
        .without("host")
        .all()
        .map(p => p.key -> Tag.unwrapValue(p).toString),
      Kamon.filter("kamon.workerFlowWorkerReporter.environment-tags.filter"),
      reporterConfig.getStringList("metricsList").asScala.toList
    )
  }

  implicit class QuoteInterp(val sc: StringContext) extends AnyVal {
    def quote(args: Any*): String = "\"" + sc.s(args: _*) + "\""
  }

  def readTimeUnit(unit: String): MeasurementUnit =
    unit match {
      case "s"  => time.seconds
      case "ms" => time.milliseconds
      case "µs" => time.microseconds
      case "ns" => time.nanoseconds
      case other =>
        sys.error(s"Invalid time unit setting [$other], the possible values are [s, ms, µs, ns]")
    }

  def readInformationUnit(unit: String): MeasurementUnit =
    unit match {
      case "b"  => information.bytes
      case "kb" => information.kilobytes
      case "mb" => information.megabytes
      case "gb" => information.gigabytes
      case other =>
        sys.error(s"Invalid time unit setting [$other], the possible values are [b, kb, mb, gb]")
    }

  case class Configuration(
      timeUnit: MeasurementUnit,
      informationUnit: MeasurementUnit,
      extraTags: Seq[(String, String)],
      tagFilter: Filter,
      metricList: List[String]
  )
}
