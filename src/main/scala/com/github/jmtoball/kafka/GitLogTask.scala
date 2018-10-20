package com.github.jmtoball.kafka

import java.io.File
import java.util
import java.util.Collections

import org.apache.kafka.common.utils.{AppInfoParser, SystemTime}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.AnyObjectId
import org.slf4j.{Logger, LoggerFactory}

class GitLogTask extends SourceTask {
  private val logger: Logger = LoggerFactory.getLogger(classOf[GitLogTask])

  val SCHEMA = SchemaBuilder
    .struct()
    .name("gitCcommit")
    .field("authorName", Schema.OPTIONAL_STRING_SCHEMA)
    .field("authorEmail", Schema.OPTIONAL_STRING_SCHEMA)
    .field("message", Schema.OPTIONAL_STRING_SCHEMA)
    .build()

  private var path: String = _
  private var topic: String = _
  private var interval: Int = _
  private var lastChecked: Long = -1
  private var lastVersion: Option[AnyObjectId] = None
  private var repo: Git = _

  override def start(props: util.Map[String, String]): Unit = {
    logger.info("Task config:")
    props.forEach((k, s) => logger.info((k, s).toString))
    topic = props.get("topic")
    interval = props.get("interval").toInt
    path = props.get("path")
    repo = Git.open(new File(path))
  }

  override def poll(): util.List[SourceRecord] = {
    val timeRemaining = new SystemTime().milliseconds() - lastChecked
    if (timeRemaining <= 0) {
      Thread.sleep(timeRemaining)
      null
    } else {
      lastChecked = new SystemTime().milliseconds
      val repoVersion = new GitRepoVersion(repo)
      repoVersion.update()
      val partition = Collections.singletonMap("path", path)
      val records = new util.ArrayList[SourceRecord]()
      logger.info("Checking repo", repo)
      new GitLogWalker(repo).walk(lastVersion).forEachRemaining(commit => {
        val commitData = new Struct(SCHEMA)
        val sourceOffset = Collections.singletonMap("sha", commit.getCommitTime)
        commitData.put("authorName", commit.getAuthorIdent.getName)
        commitData.put("authorEmail", commit.getAuthorIdent.getEmailAddress)
        commitData.put("message", commit.getFullMessage)
        records.add(new SourceRecord(partition, sourceOffset, topic, SCHEMA, commitData))
      })
      lastVersion = Some(repoVersion.currentVersion())
      records
    }
  }

  override def stop(): Unit = {

  }

  override def version(): String = AppInfoParser.getVersion
}
