package repcheck.ingestion.common.execution

/**
 * Updates a pipeline application's own execution state in the `workflow_run_steps` table.
 *
 * Each pipeline owns its row(s) — the Launcher only reads this table to check dependencies. The trait exists so that
 * applications can inject a Doobie-backed implementation in production and a mock/in-memory implementation in tests.
 */
trait WorkflowStateUpdater[F[_]] {

  /**
   * Mark a step as running. Must be idempotent — on a retry the same (runId, stepName) row is updated in place rather
   * than a new row inserted. Sets `status = Running` and `started_at = now`.
   */
  def recordStepStarted(runId: String, stepName: String): F[Unit]

  /**
   * Mark a step as completed. Sets `status = Completed` and `completed_at = now`.
   */
  def recordStepCompleted(runId: String, stepName: String): F[Unit]

  /**
   * Mark a step as failed. Sets `status = Failed`, `completed_at = now`, and `error_message = error`.
   */
  def recordStepFailed(runId: String, stepName: String, error: String): F[Unit]

  /**
   * Increment `retry_count` on the row and return the new value. Useful for deciding whether to requeue the original
   * message or mark the step permanently failed.
   */
  def incrementRetryCount(runId: String, stepName: String): F[Int]

  /**
   * Read the current `retry_count`. Returns 0 when no row exists — this matches the semantics of a brand new step that
   * has never been started.
   */
  def getRetryCount(runId: String, stepName: String): F[Int]

}
