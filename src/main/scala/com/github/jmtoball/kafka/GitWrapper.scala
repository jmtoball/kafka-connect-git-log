package com.github.jmtoball.kafka

import java.io.File
import scala.collection.JavaConverters._

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.ResetCommand.ResetType
import org.eclipse.jgit.lib.{AnyObjectId, Constants}
import org.eclipse.jgit.revwalk.{RevCommit, RevSort, RevWalk}

class GitWrapper(val open: String => Git = f => Git.open(new File(f))) {
  private var gitInstance: Git = _

  def setPath(path: String): Unit = {
    gitInstance = open(path)
  }

  def version(): AnyObjectId = {
    gitInstance.getRepository
      .resolve(Constants.HEAD).asInstanceOf[AnyObjectId]
  }

  def update(): Unit = {
    gitInstance.fetch().call()
    // TODO: Support other branches than master
    gitInstance.checkout().setName("origin/master")
    gitInstance.reset().setMode(ResetType.HARD).setRef("origin/master").call()
  }

  def walk(start: Option[AnyObjectId]): Iterator[RevCommit] = {
    val repo = gitInstance.getRepository
    val walker = new RevWalk(repo)
    walker.sort(RevSort.REVERSE, true)
    walker.markStart(walker.lookupCommit(repo.resolve(Constants.HEAD)))
    start match {
      case Some(id) => walker.markUninteresting(walker.lookupCommit(id))
      case None =>
    }
    walker.iterator().asScala
  }

}
