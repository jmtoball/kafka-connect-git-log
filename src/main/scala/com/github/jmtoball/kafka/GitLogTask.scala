package com.github.jmtoball.kafka

import java.time.Instant
import java.util
import java.util.Collections
import scala.collection.JavaConverters._

import com.github.jmtoball.kafka.conf.{Config, TaskConfig}
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.common.utils.{AppInfoParser, SystemTime}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.eclipse.jgit.lib.AnyObjectId
import org.eclipse.jgit.revwalk.RevCommit
import org.slf4j.{Logger, LoggerFactory}

class GitLogTask() extends SourceTask {
  private val logger: Logger = LoggerFactory.getLogger(classOf[GitLogTask])

  private var config: AbstractConfig = _
  private var path: String = _
  private var topic: String = _
  private var interval: Int = _
  private var pull: Boolean = _
  private var rewind: Boolean = _
  private var lastChecked: Long = new SystemTime().milliseconds()
  private var lastVersion: Option[AnyObjectId] = None
  val git: GitWrapper = new GitWrapper

  override def stop(): Unit = {}

  override def version(): String = AppInfoParser.getVersion

  override def start(props: util.Map[String, String]): Unit = {
    config = new TaskConfig(props)
    topic = config.getString(Config.KEY_TOPIC)
    interval = config.getInt(Config.KEY_INTERVAL)
    pull = config.getBoolean(Config.KEY_PULL)
    rewind = config.getBoolean(Config.KEY_REWIND)
    path = config.getString(Config.KEY_PATH)
    git.setPath(path)
  }

  override def poll(): util.List[SourceRecord] = {
    val timeRemaining = lastChecked + interval - new SystemTime().milliseconds()
    println(s"Time remaining $timeRemaining. Interval: $interval")
    if (timeRemaining > 0) {
      Thread.sleep(timeRemaining)
      null
    } else {
      if (pull) { git.update() }
      logger.info(s"Checking repo in $path")
      lastChecked = new SystemTime().milliseconds
      val records = git.walk(start=lastVersion).map(createRecord)
      lastVersion = Some(git.version())
      records.toList.asJava
    }
  }

  val SCHEMA = SchemaBuilder
    .struct()
    .name("gitCommit")
    .field("authorName", Schema.OPTIONAL_STRING_SCHEMA)
    .field("authorEmail", Schema.OPTIONAL_STRING_SCHEMA)
    .field("committerName", Schema.OPTIONAL_STRING_SCHEMA)
    .field("committerEmail", Schema.OPTIONAL_STRING_SCHEMA)
    .field("message", Schema.OPTIONAL_STRING_SCHEMA)
    .field("date", Schema.OPTIONAL_STRING_SCHEMA)
    .build()

  private def createRecord(commit: RevCommit): SourceRecord = {
    val partition = Collections.singletonMap("path", path)
    val commitData = new Struct(SCHEMA)
    val sourceOffset = Collections.singletonMap("unixTime", commit.getCommitTime)
    commitData.put("authorName", commit.getAuthorIdent.getName)
    commitData.put("authorEmail", commit.getAuthorIdent.getEmailAddress)
    commitData.put("committerName", commit.getCommitterIdent.getName)
    commitData.put("committerEmail", commit.getCommitterIdent.getEmailAddress)
    commitData.put("message", commit.getFullMessage)
    commitData.put("date", Instant.ofEpochSecond(commit.getCommitTime).toString)
    new SourceRecord(partition, sourceOffset, topic, SCHEMA, commitData)
  }
}
