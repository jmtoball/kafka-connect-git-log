package com.github.jmtoball.kafka

import org.eclipse.jgit.api.Git
import org.scalatest._
import org.junit.runner.RunWith
import org.scalamock.scalatest.MockFactory
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class GitWrapperTest extends FunSpec with Matchers with MockFactory with GitHelp with BeforeAndAfter {
  private var subject: GitWrapper = _
  private var testRepo: Git = _


  before {
    testRepo = createTestRepo()
    subject = new GitWrapper(_ => testRepo)
    subject.setPath(null)
  }

  describe("#version") {
    it("should return null for an empty repo") {
      println(subject.version())
      subject.version() should be (null)
    }

    it("should return the current checked out version in the repo") {
      testRepo.commit().setAuthor("Hans Dampf", "Hans.Dampf@nihil.foo").setCommitter("Karl Rauch", "karl.rauch@nihil.foo").setMessage("Lorem ipsum!").call()
      println(subject.version())
      subject.version() should not be null
    }
  }
}
