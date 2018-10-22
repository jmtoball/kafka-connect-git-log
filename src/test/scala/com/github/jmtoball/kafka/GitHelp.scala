package com.github.jmtoball.kafka

import java.io.File

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.storage.file.FileRepositoryBuilder

trait GitHelp {

  def createTestRepo(): Git = {
    val tempDir = File.createTempFile("kafka-connect-git-log", "tmp")
    tempDir.delete()
    val tempGitDir = new File(tempDir.getAbsolutePath, ".git")
    FileRepositoryBuilder.create(tempGitDir).create(false)
    Git.open(tempDir)
  }
}
