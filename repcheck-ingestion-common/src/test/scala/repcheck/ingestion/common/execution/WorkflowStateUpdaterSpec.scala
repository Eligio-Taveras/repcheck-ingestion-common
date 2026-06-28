package repcheck.ingestion.common.execution

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.ids.RunId
import repcheck.ingestion.common.testing.{DockerPostgresSpec, DockerRequired}

class WorkflowStateUpdaterSpec extends AnyFlatSpec with Matchers with DockerPostgresSpec {

  private val testConfig = PipelineFailureHandlerConfig(maxRetries = 3)

  /** Insert a parent workflow_runs row and return its auto-generated id. */
  private def createWorkflowRun(xa: Transactor[IO], name: String): IO[Long] =
    sql"""INSERT INTO workflow_runs (workflow_name, status, triggered_by) VALUES ($name, 'running', 'manual')""".update
      .withUniqueGeneratedKeys[Long]("id")
      .transact(xa)

  private def withFixture[A](block: (Transactor[IO], WorkflowStateUpdater[IO], RunId) => IO[A]): A =
    TransactorResource
      .makeTransactor[IO](
        driverClassName = "org.postgresql.Driver",
        url = jdbcUrl,
        user = jdbcUser,
        pass = jdbcPassword,
        maxConnections = 4,
      )
      .use { xa =>
        for {
          _      <- sql"TRUNCATE TABLE workflow_run_steps, workflow_runs CASCADE".update.run.transact(xa)
          runId  <- createWorkflowRun(xa, "test-workflow")
          result <- block(xa, new WorkflowStateUpdater[IO](xa, testConfig), RunId(runId))
        } yield result
      }
      .unsafeRunSync()

  private def readStatus(xa: Transactor[IO], runId: RunId, step: String): IO[Option[String]] = {
    val id = runId.value
    sql"""SELECT status FROM workflow_run_steps WHERE workflow_run_id = $id AND step_name = $step"""
      .query[String]
      .option
      .transact(xa)
  }

  private def readCompletedAtPresent(xa: Transactor[IO], runId: RunId, step: String): IO[Boolean] = {
    val id = runId.value
    sql"""SELECT (completed_at IS NOT NULL) FROM workflow_run_steps WHERE workflow_run_id = $id AND step_name = $step"""
      .query[Boolean]
      .unique
      .transact(xa)
  }

  private def readErrorMessage(xa: Transactor[IO], runId: RunId, step: String): IO[Option[String]] = {
    val id = runId.value
    sql"""SELECT error_message FROM workflow_run_steps WHERE workflow_run_id = $id AND step_name = $step"""
      .query[Option[String]]
      .unique
      .transact(xa)
  }

  private def countRows(xa: Transactor[IO], runId: RunId, step: String): IO[Int] = {
    val id = runId.value
    sql"""SELECT COUNT(*) FROM workflow_run_steps WHERE workflow_run_id = $id AND step_name = $step"""
      .query[Int]
      .unique
      .transact(xa)
  }

  "recordStepStarted" should "insert a new row with status Running" taggedAs DockerRequired in {
    withFixture { (xa, updater, runId) =>
      val step = "members-pipeline"
      for {
        _      <- updater.recordStepStarted(runId, step)
        status <- readStatus(xa, runId, step)
      } yield status shouldBe Some("running")
    }
  }

  it should "be idempotent on a retry — ON CONFLICT updates in place, no duplicate rows" taggedAs DockerRequired in {
    withFixture { (xa, updater, runId) =>
      val step = "votes-pipeline"
      for {
        _     <- updater.recordStepStarted(runId, step)
        _     <- updater.recordStepStarted(runId, step)
        count <- countRows(xa, runId, step)
      } yield count shouldBe 1
    }
  }

  "recordStepCompleted" should "update status to Completed and set completed_at" taggedAs DockerRequired in {
    withFixture { (xa, updater, runId) =>
      val step = "bills-pipeline"
      for {
        _              <- updater.recordStepStarted(runId, step)
        _              <- updater.recordStepCompleted(runId, step)
        status         <- readStatus(xa, runId, step)
        completedAtSet <- readCompletedAtPresent(xa, runId, step)
      } yield {
        val _ = status shouldBe Some("completed")
        completedAtSet shouldBe true
      }
    }
  }

  "recordStepFailed" should "update status to Failed and set error_message" taggedAs DockerRequired in {
    withFixture { (xa, updater, runId) =>
      val step = "amendments-pipeline"
      for {
        _            <- updater.recordStepStarted(runId, step)
        _            <- updater.recordStepFailed(runId, step, "connection timeout")
        status       <- readStatus(xa, runId, step)
        errorMessage <- readErrorMessage(xa, runId, step)
      } yield {
        val _ = status shouldBe Some("failed")
        errorMessage shouldBe Some("connection timeout")
      }
    }
  }

  "incrementRetryCount" should "increment starting from 0 and return the new count" taggedAs DockerRequired in {
    withFixture { (_, updater, runId) =>
      val step = "members-pipeline"
      for {
        _     <- updater.recordStepStarted(runId, step)
        one   <- updater.incrementRetryCount(runId, step)
        two   <- updater.incrementRetryCount(runId, step)
        three <- updater.incrementRetryCount(runId, step)
      } yield {
        val _ = one shouldBe 1
        val _ = two shouldBe 2
        three shouldBe 3
      }
    }
  }

  "getRetryCount" should "return 0 when the step has never been recorded" taggedAs DockerRequired in {
    withFixture((_, updater, _) => updater.getRetryCount(RunId(99999L), "never-seen").map(_ shouldBe 0))
  }

  it should "return the current value after increments" taggedAs DockerRequired in {
    withFixture { (_, updater, runId) =>
      val step = "votes-pipeline"
      for {
        _       <- updater.recordStepStarted(runId, step)
        _       <- updater.incrementRetryCount(runId, step)
        _       <- updater.incrementRetryCount(runId, step)
        current <- updater.getRetryCount(runId, step)
      } yield current shouldBe 2
    }
  }

  "multiple steps on the same run" should "be tracked independently" taggedAs DockerRequired in {
    withFixture { (xa, updater, runId) =>
      val stepA = "step-a"
      val stepB = "step-b"
      for {
        _       <- updater.recordStepStarted(runId, stepA)
        _       <- updater.recordStepStarted(runId, stepB)
        _       <- updater.recordStepCompleted(runId, stepA)
        _       <- updater.recordStepFailed(runId, stepB, "boom")
        statusA <- readStatus(xa, runId, stepA)
        statusB <- readStatus(xa, runId, stepB)
      } yield {
        val _ = statusA shouldBe Some("completed")
        statusB shouldBe Some("failed")
      }
    }
  }

}
