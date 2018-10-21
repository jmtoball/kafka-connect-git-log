package com.github.jmtoball.kafka

import java.util

import com.github.jmtoball.kafka.conf.{Config, ConnectorConfig}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConverters._

class GitLogConnector extends SourceConnector {
  private var paths: List[String] = _
  private var topic: String = _
  private var interval: Int = _
  private var configReader: AbstractConfig = _
  private var rewind: Boolean = _
  private var pull: Boolean = _

  override def version(): String = AppInfoParser.getVersion

  override def start(props: util.Map[String, String]): Unit = {
    configReader = new ConnectorConfig(props)
    topic = configReader.getString(Config.KEY_TOPIC)
    if (topic == null)
      throw new ConnectException("missing topic config value")

    interval = configReader.getInt(Config.KEY_INTERVAL)
    if (!interval.isValidInt)
      throw new ConnectException("invalid interval config value")

    paths = configReader.getList(Config.KEY_PATHS).asScala.toList
    if (paths.isEmpty)
      throw new ConnectException(s"${Config.KEY_PATHS} must contain at least one path")
    if (paths.map(s => s.trim).exists(s => s.isEmpty))
      throw new ConnectException(s"paths in ${Config.KEY_PATHS} must not be empty")

    rewind = configReader.getBoolean(Config.KEY_REWIND)
    pull = configReader.getBoolean(Config.KEY_PULL)
  }

  override def taskClass(): Class[_ <: Task] = classOf[GitLogTask]

  override def taskConfigs(i: Int): util.List[util.Map[String, String]] = {
    paths.map(path =>
      Map(
        Config.KEY_PATH -> path,
        Config.KEY_TOPIC -> topic,
        Config.KEY_INTERVAL -> interval.toString,
        Config.KEY_REWIND -> rewind.toString,
        Config.KEY_PULL -> pull.toString
      ).asJava
    ).asJava
  }

  override def stop(): Unit = {}

  override def config(): ConfigDef = {
    Config.connectorConfigDef
  }
}
