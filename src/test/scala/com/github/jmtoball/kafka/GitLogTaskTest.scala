package com.github.jmtoball.kafka

import java.io.File
import java.nio.file.Paths
import java.time.Instant

import org.scalatest._
import java.util

import scala.collection.JavaConverters._
import com.github.jmtoball.kafka.conf.Config
import org.apache.kafka.common.utils.SystemTime
import org.apache.kafka.connect.data.Struct
import org.eclipse.jgit.api.Git
import org.eclipse.jgit.internal.storage.file.FileRepository
import org.eclipse.jgit.lib.{AnyObjectId, ObjectId}
import org.eclipse.jgit.revwalk.RevWalk
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GitLogTaskTest extends FunSpec with Matchers with MockFactory with BeforeAndAfter {
  private val testPath = "foobar"
  private var gitAccessMock: GitWrapper = _
  private var task: GitLogTask = _


  def createTestRepo(): Git = {
    val tempDir = File.createTempFile("kafka-connect-git-log", "tmp")
    tempDir.delete()
    val tempGitDir = new File(tempDir.getAbsolutePath, ".git")
    FileRepositoryBuilder.create(tempGitDir).create(false)
    Git.open(tempDir)
  }

  def props(overrides: Map[String, String]= Map()): util.Map[String, String] = {
    (Map(
      Config.KEY_TOPIC -> "test",
      Config.KEY_INTERVAL -> 0.toString,
      Config.KEY_PULL -> "false",
      Config.KEY_PATH -> testPath
    ) ++ overrides).asJava
  }

  before {
    gitAccessMock = mock[GitWrapper]
    task = new GitLogTask {
      override val git: GitWrapper = gitAccessMock
    }
  }

  describe("#version") {
    it("should return a version") {
      task.version should not be null
      task.version should not be empty
    }
  }

  describe("#start") {
    it("should open a git repo at the given path") {
      gitAccessMock.setPath _ expects testPath once()
      task.start(props())
    }
  }

  describe("#poll") {
    val versionSHA1 = "f6052354cfb5737043e3e5f079cd777e393a0000"
    val versionSHA2 = "f6052354cfb5737043e3e5f079cd777e393a0001"
    val version1 = ObjectId.fromString(versionSHA1).asInstanceOf[AnyObjectId]
    val version2 = ObjectId.fromString(versionSHA2).asInstanceOf[AnyObjectId]

    it("walks the log with no starting point by default") {
      gitAccessMock.setPath _ expects testPath once()
      task.start(props())
      gitAccessMock.walk _ expects None once() returning List().toIterator
      gitAccessMock.version _ expects() once() returning version1
      task.poll()
    }

    it("starts with the last checked version on consecutive calls") {
      gitAccessMock.setPath _ expects testPath once()
      task.start(props())
      gitAccessMock.walk _ expects None once() returning List().toIterator
      gitAccessMock.version _ expects() once() returning version1
      task.poll()
      gitAccessMock.walk _ expects Some(version1) once() returning List().toIterator
      gitAccessMock.version _ expects() once() returning version2
      task.poll()
    }

    describe("with data") {
      val repo = createTestRepo()
      val commits = List(
        repo.commit().setAuthor("Hans Dampf", "Hans.Dampf@nihil.foo").setCommitter("Karl Rauch", "karl.rauch@nihil.foo").setMessage("Lorem ipsum!").call(),
        repo.commit().setAuthor("Hans Dampf", "Hans.Dampf@nihil.foo").setCommitter("Karl Rauch", "karl.rauch@nihil.foo").setMessage("Dolor sit amet.").call()
      )
      it("produces source records for the data") {
        gitAccessMock.setPath _ expects testPath once()
        task.start(props())
        gitAccessMock.walk _ expects None once() returning commits.toIterator
        gitAccessMock.version _ expects() once() returning version1
        val records = task.poll().asScala
        records should have size commits.length
        records.zip(commits).foreach { case (record, commit) => {
          val r = record.value.asInstanceOf[Struct]
          r.getString("authorName") should be (commit.getAuthorIdent.getName)
          r.getString("authorEmail") should be (commit.getAuthorIdent.getEmailAddress)
          r.getString("committerName") should be (commit.getCommitterIdent.getName)
          r.getString("committerEmail") should be (commit.getCommitterIdent.getEmailAddress)
          r.getString("message") should be (commit.getFullMessage)
          r.getString("date") should be (Instant.ofEpochSecond(commit.getCommitTime).toString)
        }}
      }
    }

    describe("with a longer interval") {
      val testInterval = 100
      it("returns null and sleeps") {
        gitAccessMock.setPath _ expects testPath once()
        val timeBefore = new SystemTime().milliseconds()
        task.start(props(Map(Config.KEY_INTERVAL -> testInterval.toString)))
        task.poll()
        val timeAfter = new SystemTime().milliseconds()
        val elapsedTime = timeAfter - timeBefore
        elapsedTime should be >= testInterval.toLong
      }
    }

    describe("with the pull-option set") {
      it("updates") {
        gitAccessMock.setPath _ expects testPath once()
        task.start(props(Map(Config.KEY_PULL -> true.toString)))
        gitAccessMock.walk _ expects None once() returning List().toIterator
        gitAccessMock.version _ expects () once() returning version1

        gitAccessMock.update _ expects () once()

        task.poll()
      }
    }
  }
}

