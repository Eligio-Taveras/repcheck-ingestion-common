package repcheck.ingestion.common.execution

import java.time.Instant
import java.util.UUID

import scala.util.control.NonFatal

import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._

import doobie.Fragment
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.errors.RunIdMissing
import repcheck.pipeline.models.constants.Tables
import repcheck.pipeline.models.workflow.state.WorkflowStepStatus

/**
 * Doobie-backed [[WorkflowStateUpdater]] targeting the `workflow_run_steps` table defined in Component 2 §2.6.
 *
 * The row is keyed by `(workflow_run_id, step_name)`. `recordStepStarted` upserts — it either inserts a brand new row
 * (status `Running`) or updates the existing row in place, which matches the spec's requirement that each step has a
 * single row with status updated in place across retries.
 *
 * `workflow_run_id` is a `UUID` column in AlloyDB/Postgres. The trait signature uses `String` because pipeline
 * applications typically receive the run ID as a CLI argument; this implementation parses it once per call and raises
 * [[RunIdMissing]] if the string is not a valid UUID, keeping the caller free of UUID plumbing.
 */
class DoobieWorkflowStateUpdater[F[_]: MonadCancelThrow](xa: Transactor[F]) extends WorkflowStateUpdater[F] {

  private val table: Fragment = Fragment.const(s""""${Tables.WorkflowRunSteps}"""")

  private def parseRunId(runId: String): F[UUID] =
    try MonadCancelThrow[F].pure(UUID.fromString(runId))
    catch {
      case NonFatal(ex) =>
        MonadCancelThrow[F].raiseError[UUID](
          RunIdMissing(s"'$runId' is not a valid UUID: ${ex.getMessage}")
        )
    }

  override def recordStepStarted(runId: String, stepName: String): F[Unit] =
    parseRunId(runId).flatMap { uuid =>
      val now                         = Instant.now()
      val running: WorkflowStepStatus = WorkflowStepStatus.Running
      val updateSql =
        fr"""UPDATE """ ++ table ++
          fr"""SET status = $running, started_at = $now, updated_at = $now
              WHERE workflow_run_id = $uuid AND step_name = $stepName"""
      val insertSql =
        fr"""INSERT INTO """ ++ table ++
          fr"""(workflow_run_id, step_name, status, retry_count, max_retries, started_at, created_at, updated_at)
              VALUES ($uuid, $stepName, $running, 0, 3, $now, $now, $now)"""
      val program = for {
        updated <- updateSql.update.run
        _ <-
          if (updated == 0) { insertSql.update.run }
          else { 0.pure[doobie.ConnectionIO] }
      } yield ()
      program.transact(xa)
    }

  override def recordStepCompleted(runId: String, stepName: String): F[Unit] =
    parseRunId(runId).flatMap { uuid =>
      val now                           = Instant.now()
      val completed: WorkflowStepStatus = WorkflowStepStatus.Completed
      val sql =
        fr"""UPDATE """ ++ table ++
          fr"""SET status = $completed, completed_at = $now, updated_at = $now
              WHERE workflow_run_id = $uuid AND step_name = $stepName"""
      sql.update.run.transact(xa).void
    }

  override def recordStepFailed(runId: String, stepName: String, error: String): F[Unit] =
    parseRunId(runId).flatMap { uuid =>
      val now                        = Instant.now()
      val failed: WorkflowStepStatus = WorkflowStepStatus.Failed
      val sql =
        fr"""UPDATE """ ++ table ++
          fr"""SET status = $failed, completed_at = $now, updated_at = $now, error_message = $error
              WHERE workflow_run_id = $uuid AND step_name = $stepName"""
      sql.update.run.transact(xa).void
    }

  override def incrementRetryCount(runId: String, stepName: String): F[Int] =
    parseRunId(runId).flatMap { uuid =>
      val now = Instant.now()
      val sql =
        fr"""UPDATE """ ++ table ++
          fr"""SET retry_count = retry_count + 1, updated_at = $now
              WHERE workflow_run_id = $uuid AND step_name = $stepName"""
      for {
        _     <- sql.update.run.transact(xa)
        count <- getRetryCountInternal(uuid, stepName)
      } yield count
    }

  override def getRetryCount(runId: String, stepName: String): F[Int] =
    parseRunId(runId).flatMap(uuid => getRetryCountInternal(uuid, stepName))

  private def getRetryCountInternal(uuid: UUID, stepName: String): F[Int] = {
    val sql =
      fr"""SELECT retry_count FROM """ ++ table ++
        fr"""WHERE workflow_run_id = $uuid AND step_name = $stepName"""
    sql.query[Int].option.transact(xa).map(_.getOrElse(0))
  }

}
