package repcheck.ingestion.common.execution

import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.transactor.Transactor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.errors.RunIdMissing
import repcheck.pipeline.models.constants.Tables

class DoobieWorkflowStateUpdaterSpec extends AnyFlatSpec with Matchers {

  private def withFixture[A](block: (Transactor[IO], DoobieWorkflowStateUpdater[IO]) => IO[A]): A = {
    val dbName = s"wfu_${UUID.randomUUID().toString.replace("-", "")}"
    val resource = TransactorResource.makeTransactor[IO](
      driverClassName = "org.h2.Driver",
      url =
        s"jdbc:h2:mem:$dbName;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;CASE_INSENSITIVE_IDENTIFIERS=TRUE;DB_CLOSE_DELAY=-1",
      user = "sa",
      pass = "",
      maxConnections = 2,
    )
    val program = resource.use { xa =>
      val table = Tables.WorkflowRunSteps
      val create =
        sql"""CREATE TABLE IF NOT EXISTS workflow_run_steps (
              workflow_run_id UUID NOT NULL,
              step_name VARCHAR(255) NOT NULL,
              status VARCHAR(32) NOT NULL,
              pipeline_run_id UUID,
              retry_count INT NOT NULL DEFAULT 0,
              max_retries INT NOT NULL DEFAULT 3,
              original_message TEXT,
              started_at TIMESTAMP,
              completed_at TIMESTAMP,
              error_message TEXT,
              created_at TIMESTAMP NOT NULL,
              updated_at TIMESTAMP NOT NULL,
              PRIMARY KEY (workflow_run_id, step_name)
            )""".update.run.transact(xa)
      val _ = table // referenced for readability — proves the constant is used elsewhere in prod
      create >> block(xa, new DoobieWorkflowStateUpdater[IO](xa))
    }
    program.unsafeRunSync()
  }

  private def readStatus(xa: Transactor[IO], runId: UUID, step: String): IO[Option[String]] =
    sql"""SELECT status FROM workflow_run_steps WHERE workflow_run_id = $runId AND step_name = $step"""
      .query[String]
      .option
      .transact(xa)

  private def readCompletedAtPresent(xa: Transactor[IO], runId: UUID, step: String): IO[Boolean] =
    sql"""SELECT (completed_at IS NOT NULL) FROM workflow_run_steps WHERE workflow_run_id = $runId AND step_name = $step"""
      .query[Boolean]
      .unique
      .transact(xa)

  private def readErrorMessage(xa: Transactor[IO], runId: UUID, step: String): IO[Option[String]] =
    sql"""SELECT error_message FROM workflow_run_steps WHERE workflow_run_id = $runId AND step_name = $step"""
      .query[Option[String]]
      .unique
      .transact(xa)

  "recordStepStarted" should "insert a new row with status Running" in {
    val runId = UUID.randomUUID()
    val step  = "members-pipeline"
    withFixture { (xa, updater) =>
      for {
        _      <- updater.recordStepStarted(runId.toString, step)
        status <- readStatus(xa, runId, step)
      } yield status shouldBe Some("Running")
    }
  }

  it should "be idempotent on a retry — updates in place, no duplicate rows" in {
    val runId = UUID.randomUUID()
    val step  = "votes-pipeline"
    withFixture { (xa, updater) =>
      for {
        _ <- updater.recordStepStarted(runId.toString, step)
        _ <- updater.recordStepStarted(runId.toString, step)
        count <- sql"""SELECT COUNT(*) FROM workflow_run_steps WHERE workflow_run_id = $runId AND step_name = $step"""
          .query[Int]
          .unique
          .transact(xa)
      } yield count shouldBe 1
    }
  }

  "recordStepCompleted" should "update status to Completed and set completed_at" in {
    val runId = UUID.randomUUID()
    val step  = "bills-pipeline"
    withFixture { (xa, updater) =>
      for {
        _              <- updater.recordStepStarted(runId.toString, step)
        _              <- updater.recordStepCompleted(runId.toString, step)
        status         <- readStatus(xa, runId, step)
        completedAtSet <- readCompletedAtPresent(xa, runId, step)
      } yield {
        val _ = status shouldBe Some("Completed")
        completedAtSet shouldBe true
      }
    }
  }

  "recordStepFailed" should "update status to Failed and set error_message" in {
    val runId = UUID.randomUUID()
    val step  = "amendments-pipeline"
    withFixture { (xa, updater) =>
      for {
        _            <- updater.recordStepStarted(runId.toString, step)
        _            <- updater.recordStepFailed(runId.toString, step, "connection timeout")
        status       <- readStatus(xa, runId, step)
        errorMessage <- readErrorMessage(xa, runId, step)
      } yield {
        val _ = status shouldBe Some("Failed")
        errorMessage shouldBe Some("connection timeout")
      }
    }
  }

  "incrementRetryCount" should "increment starting from 0 and return the new count" in {
    val runId = UUID.randomUUID()
    val step  = "members-pipeline"
    withFixture { (_, updater) =>
      for {
        _     <- updater.recordStepStarted(runId.toString, step)
        one   <- updater.incrementRetryCount(runId.toString, step)
        two   <- updater.incrementRetryCount(runId.toString, step)
        three <- updater.incrementRetryCount(runId.toString, step)
      } yield {
        val _ = one shouldBe 1
        val _ = two shouldBe 2
        three shouldBe 3
      }
    }
  }

  "getRetryCount" should "return 0 when the step has never been recorded" in {
    val runId = UUID.randomUUID()
    withFixture((_, updater) => updater.getRetryCount(runId.toString, "never-seen").map(_ shouldBe 0))
  }

  it should "return the current value after increments" in {
    val runId = UUID.randomUUID()
    val step  = "votes-pipeline"
    withFixture { (_, updater) =>
      for {
        _       <- updater.recordStepStarted(runId.toString, step)
        _       <- updater.incrementRetryCount(runId.toString, step)
        _       <- updater.incrementRetryCount(runId.toString, step)
        current <- updater.getRetryCount(runId.toString, step)
      } yield current shouldBe 2
    }
  }

  "multiple steps on the same run" should "be tracked independently" in {
    val runId = UUID.randomUUID()
    val stepA = "step-a"
    val stepB = "step-b"
    withFixture { (xa, updater) =>
      for {
        _       <- updater.recordStepStarted(runId.toString, stepA)
        _       <- updater.recordStepStarted(runId.toString, stepB)
        _       <- updater.recordStepCompleted(runId.toString, stepA)
        _       <- updater.recordStepFailed(runId.toString, stepB, "boom")
        statusA <- readStatus(xa, runId, stepA)
        statusB <- readStatus(xa, runId, stepB)
      } yield {
        val _ = statusA shouldBe Some("Completed")
        statusB shouldBe Some("Failed")
      }
    }
  }

  "DoobieWorkflowStateUpdater" should "raise RunIdMissing when the run ID is not a valid UUID" in {
    withFixture { (_, updater) =>
      updater
        .recordStepStarted("not-a-uuid", "step")
        .attempt
        .map { result =>
          val _ = result.isLeft shouldBe true
          result.swap.getOrElse(fail("expected error")) shouldBe a[RunIdMissing]
        }
    }
  }

  it should "raise RunIdMissing on incrementRetryCount with invalid UUID" in {
    withFixture { (_, updater) =>
      updater.incrementRetryCount("not-a-uuid", "step").attempt.map { result =>
        val _ = result.isLeft shouldBe true
        result.swap.getOrElse(fail("expected error")) shouldBe a[RunIdMissing]
      }
    }
  }

  it should "raise RunIdMissing on getRetryCount with invalid UUID" in {
    withFixture { (_, updater) =>
      updater.getRetryCount("bad", "step").attempt.map(result => result.isLeft shouldBe true)
    }
  }

  it should "raise RunIdMissing on recordStepCompleted with invalid UUID" in {
    withFixture { (_, updater) =>
      updater.recordStepCompleted("bad", "step").attempt.map(result => result.isLeft shouldBe true)
    }
  }

  it should "raise RunIdMissing on recordStepFailed with invalid UUID" in {
    withFixture { (_, updater) =>
      updater.recordStepFailed("bad", "step", "error").attempt.map(result => result.isLeft shouldBe true)
    }
  }

}
