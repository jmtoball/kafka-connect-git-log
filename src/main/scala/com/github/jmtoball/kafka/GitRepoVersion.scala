package com.github.jmtoball.kafka

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.api.ResetCommand.ResetType
import org.eclipse.jgit.lib.{AnyObjectId, Constants, ObjectId}

class GitRepoVersion (private val repo: Git) {

  def update(): Unit = {
    repo.fetch().call()
    // TODO: Support other branches than master
    repo.checkout().setName("origin/master")
    repo.reset().setMode(ResetType.HARD).setRef("origin/master").call()
  }

  def forSHA(sha: String): AnyObjectId = {
    repo.getRepository.resolve(sha).asInstanceOf[AnyObjectId]
  }

  def currentVersion(): AnyObjectId = {
    forSHA(Constants.HEAD)
  }
}
