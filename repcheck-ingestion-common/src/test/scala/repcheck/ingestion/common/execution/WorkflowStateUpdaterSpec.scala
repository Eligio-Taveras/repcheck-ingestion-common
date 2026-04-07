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
import repcheck.ingestion.common.testing.{DockerPostgresSpec, DockerRequired}

class WorkflowStateUpdaterSpec extends AnyFlatSpec with Matchers with DockerPostgresSpec {

  private val testConfig = PipelineFailureHandlerConfig(maxRetries = 3)

  private def withFixture[A](block: (Transactor[IO], WorkflowStateUpdater[IO]) => IO[A]): A =
    TransactorResource
      .makeTransactor[IO](
        driverClassName = "org.postgresql.Driver",
        url = jdbcUrl,
        user = jdbcUser,
        pass = jdbcPassword,
        maxConnections = 4,
      )
      .use { xa =>
        // Each spec runs in isolation by truncating the table up front. The table is created once
        // by the migration applied during DockerPostgresSpec startup.
        sql"TRUNCATE TABLE workflow_run_steps".update.run.transact(xa) >>
          block(xa, new WorkflowStateUpdater[IO](xa, testConfig))
      }
      .unsafeRunSync()

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

  private def countRows(xa: Transactor[IO], runId: UUID, step: String): IO[Int] =
    sql"""SELECT COUNT(*) FROM workflow_run_steps WHERE workflow_run_id = $runId AND step_name = $step"""
      .query[Int]
      .unique
      .transact(xa)

  "recordStepStarted" should "insert a new row with status Running" taggedAs DockerRequired in {
    val runId = UUID.randomUUID()
    val step  = "members-pipeline"
    withFixture { (xa, updater) =>
      for {
        _      <- updater.recordStepStarted(runId.toString, step)
        status <- readStatus(xa, runId, step)
      } yield status shouldBe Some("Running")
    }
  }

  it should "be idempotent on a retry — ON CONFLICT updates in place, no duplicate rows" taggedAs DockerRequired in {
    val runId = UUID.randomUUID()
    val step  = "votes-pipeline"
    withFixture { (xa, updater) =>
      for {
        _     <- updater.recordStepStarted(runId.toString, step)
        _     <- updater.recordStepStarted(runId.toString, step)
        count <- countRows(xa, runId, step)
      } yield count shouldBe 1
    }
  }

  "recordStepCompleted" should "update status to Completed and set completed_at" taggedAs DockerRequired in {
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

  "recordStepFailed" should "update status to Failed and set error_message" taggedAs DockerRequired in {
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

  "incrementRetryCount" should "increment starting from 0 and return the new count" taggedAs DockerRequired in {
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

  "getRetryCount" should "return 0 when the step has never been recorded" taggedAs DockerRequired in {
    val runId = UUID.randomUUID()
    withFixture((_, updater) => updater.getRetryCount(runId.toString, "never-seen").map(_ shouldBe 0))
  }

  it should "return the current value after increments" taggedAs DockerRequired in {
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

  "multiple steps on the same run" should "be tracked independently" taggedAs DockerRequired in {
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

  "WorkflowStateUpdater" should "raise RunIdMissing when the run ID is not a valid UUID" taggedAs DockerRequired in {
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

  it should "raise RunIdMissing on incrementRetryCount with invalid UUID" taggedAs DockerRequired in {
    withFixture { (_, updater) =>
      updater.incrementRetryCount("not-a-uuid", "step").attempt.map { result =>
        val _ = result.isLeft shouldBe true
        result.swap.getOrElse(fail("expected error")) shouldBe a[RunIdMissing]
      }
    }
  }

  it should "raise RunIdMissing on getRetryCount with invalid UUID" taggedAs DockerRequired in {
    withFixture { (_, updater) =>
      updater.getRetryCount("bad", "step").attempt.map(result => result.isLeft shouldBe true)
    }
  }

  it should "raise RunIdMissing on recordStepCompleted with invalid UUID" taggedAs DockerRequired in {
    withFixture { (_, updater) =>
      updater.recordStepCompleted("bad", "step").attempt.map(result => result.isLeft shouldBe true)
    }
  }

  it should "raise RunIdMissing on recordStepFailed with invalid UUID" taggedAs DockerRequired in {
    withFixture { (_, updater) =>
      updater.recordStepFailed("bad", "step", "error").attempt.map(result => result.isLeft shouldBe true)
    }
  }

}
