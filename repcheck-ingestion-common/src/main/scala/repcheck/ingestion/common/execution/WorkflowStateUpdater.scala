package repcheck.ingestion.common.execution

import java.time.Instant

import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._

import doobie.Fragment
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.ids.RunId
import repcheck.pipeline.models.constants.Tables
import repcheck.pipeline.models.workflow.state.WorkflowStepStatus

/**
 * Updates a pipeline application's own execution state in the `workflow_run_steps` table defined in Component 2 §2.6.
 * Each pipeline owns its row(s) — the Launcher only reads this table to check dependencies.
 *
 * The row is keyed by `(workflow_run_id, step_name)`. `recordStepStarted` upserts via Postgres' `INSERT ... ON CONFLICT
 * (workflow_run_id, step_name) DO UPDATE` — a single round trip that either inserts a brand-new row (status `Running`)
 * or updates the existing row in place, which matches the spec's requirement that each step has a single row with
 * status updated in place across retries.
 *
 * `workflow_run_id` is a `BIGINT` column in AlloyDB/Postgres (post PK standardization). Every public method takes the
 * run ID as a type-safe [[repcheck.ingestion.common.ids.RunId]] and binds its underlying `Long` directly — the launcher
 * contract parses args(1) into a `RunId` at startup (see `PipelineBootstrap.extractRunId`), so there is no per-call
 * String parsing here.
 *
 * The class is non-final on purpose: there is exactly one production implementation, but tests and pipelines that need
 * to inject alternate behavior can subclass and override individual methods.
 *
 * ==SQL injection contract==
 * All `$x` interpolations in `fr"..."` go through Doobie's `Put` typeclass and are bound as parameterized prepared
 * statement values — they are safe regardless of source. The only place we splice raw SQL is the table identifier via
 * `Fragment.const`. The value comes from `Tables.WorkflowRunSteps`, a compile-time `val` constant in the
 * pipeline-models library — it can never carry user input. The wrapping double quotes additionally guarantee the value
 * is parsed as an identifier, not as SQL syntax, even if a future refactor introduces special characters in the
 * constant.
 */
class WorkflowStateUpdater[F[_]: MonadCancelThrow](
  xa: Transactor[F],
  config: PipelineFailureHandlerConfig,
) {

  // SQL-injection safe: Tables.WorkflowRunSteps is a compile-time `val` constant from a trusted module — never user
  // input. The double quotes force identifier parsing.
  private val table: Fragment = Fragment.const(s""""${Tables.WorkflowRunSteps}"""")

  /**
   * Mark a step as running. Idempotent — on a retry the same `(runId, stepName)` row is updated in place rather than a
   * new row inserted. Sets `status = Running`, `started_at = now`, and `max_retries` to the configured value.
   */
  def recordStepStarted(runId: RunId, stepName: String): F[Unit] = {
    val now                         = Instant.now()
    val id                          = runId.value
    val running: WorkflowStepStatus = WorkflowStepStatus.Running
    val maxRetries                  = config.maxRetries
    val sql =
      fr"""INSERT INTO """ ++ table ++
        fr"""(workflow_run_id, step_name, status, retry_count, max_retries, started_at, created_at, updated_at)
            VALUES ($id, $stepName, $running, 0, $maxRetries, $now, $now, $now)
            ON CONFLICT (workflow_run_id, step_name) DO UPDATE
            SET status = EXCLUDED.status, started_at = EXCLUDED.started_at, updated_at = EXCLUDED.updated_at"""
    sql.update.run.transact(xa).void
  }

  /** Mark a step as completed. Sets `status = Completed` and `completed_at = now`. */
  def recordStepCompleted(runId: RunId, stepName: String): F[Unit] = {
    val now                           = Instant.now()
    val id                            = runId.value
    val completed: WorkflowStepStatus = WorkflowStepStatus.Completed
    val sql =
      fr"""UPDATE """ ++ table ++
        fr"""SET status = $completed, completed_at = $now, updated_at = $now
            WHERE workflow_run_id = $id AND step_name = $stepName"""
    sql.update.run.transact(xa).void
  }

  /** Mark a step as failed. Sets `status = Failed`, `completed_at = now`, and `error_message = error`. */
  def recordStepFailed(runId: RunId, stepName: String, error: String): F[Unit] = {
    val now                        = Instant.now()
    val id                         = runId.value
    val failed: WorkflowStepStatus = WorkflowStepStatus.Failed
    val sql =
      fr"""UPDATE """ ++ table ++
        fr"""SET status = $failed, completed_at = $now, updated_at = $now, error_message = $error
            WHERE workflow_run_id = $id AND step_name = $stepName"""
    sql.update.run.transact(xa).void
  }

  /**
   * Increment `retry_count` on the row and return the new value. Useful for deciding whether to requeue the original
   * message or mark the step permanently failed.
   */
  def incrementRetryCount(runId: RunId, stepName: String): F[Int] = {
    val now = Instant.now()
    val id  = runId.value
    val sql =
      fr"""UPDATE """ ++ table ++
        fr"""SET retry_count = retry_count + 1, updated_at = $now
            WHERE workflow_run_id = $id AND step_name = $stepName"""
    for {
      _     <- sql.update.run.transact(xa)
      count <- getRetryCountInternal(id, stepName)
    } yield count
  }

  /**
   * Read the current `retry_count`. Returns 0 when no row exists — this matches the semantics of a brand new step that
   * has never been started.
   */
  def getRetryCount(runId: RunId, stepName: String): F[Int] =
    getRetryCountInternal(runId.value, stepName)

  private def getRetryCountInternal(id: Long, stepName: String): F[Int] = {
    val sql =
      fr"""SELECT retry_count FROM """ ++ table ++
        fr"""WHERE workflow_run_id = $id AND step_name = $stepName"""
    sql.query[Int].option.transact(xa).map(_.getOrElse(0))
  }

}
