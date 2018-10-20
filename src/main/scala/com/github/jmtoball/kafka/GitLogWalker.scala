package com.github.jmtoball.kafka

import java.util

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.lib.{AnyObjectId, Constants}
import org.eclipse.jgit.revwalk.{RevCommit, RevSort, RevWalk}

class GitLogWalker (private val git: Git) {
  private val repo = git.getRepository

  def walk(start: Option[AnyObjectId]): util.Iterator[RevCommit] = {
    val walker = new RevWalk(repo)
    walker.sort(RevSort.REVERSE, true)
    walker.markStart(walker.lookupCommit(repo.resolve(Constants.HEAD)))
    start match {
      case Some(id) => walker.markUninteresting(walker.lookupCommit(id))
      case None =>
    }
    walker.iterator()
  }

}
