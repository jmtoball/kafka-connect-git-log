package com.github.jmtoball.kafka

import java.util

import scala.collection.JavaConversions._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Type
import org.apache.kafka.common.config.ConfigDef.Range
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.errors.ConnectException
import org.slf4j.{Logger, LoggerFactory}

class GitLogConnector extends SourceConnector {
  private val logger: Logger = LoggerFactory.getLogger(classOf[GitLogConnector])

  private var repoPaths: List[String] = _
  private var topic: String = _
  private var interval: String = _

  override def version(): String = AppInfoParser.getVersion

  override def start(props: util.Map[String, String]): Unit = {
    topic = props.get("topic")
    if (topic == null)
      throw new ConnectException("missing topic config value")

    interval = props.get("interval")
    if (interval == null)
      throw new ConnectException("missing interval config value")
    if (!interval.matches("""\d+"""))
      throw new ConnectException("interval needs to be an integer value")

    val repoPathsString = props.get("repo.paths")
    if (repoPathsString == null)
      throw new ConnectException("missing repo.paths config value")
    repoPaths = repoPathsString.split(",").toList
    if (repoPaths.map(s => s.trim).exists(s => s.isEmpty))
      throw new ConnectException("paths in repo.paths may not be empty")
  }

  override def taskClass(): Class[_ <: Task] = classOf[GitLogTask]

  override def taskConfigs(i: Int): util.List[util.Map[String, String]] = {
    repoPaths.map(p =>
      new util.HashMap[String, String](Map("path" -> p, "topic" -> topic, "interval" -> interval))
    )
  }

  override def stop(): Unit = {}

  override def config(): ConfigDef = {
    new ConfigDef()
      .define("topic", Type.STRING, "git-log", ConfigDef.Importance.HIGH, "The topic to publish commits to")
      .define("interval", Type.INT, 10000, Range.atLeast(0), ConfigDef.Importance.HIGH, "the interval in which to poll each for new commits")
      .define("repo.paths", Type.STRING, "", ConfigDef.Importance.HIGH, "the repository paths to watch")

  }
}
