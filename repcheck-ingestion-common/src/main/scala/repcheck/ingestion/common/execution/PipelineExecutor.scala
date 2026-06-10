package repcheck.ingestion.common.execution

import cats.effect.{Async, ExitCode}
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.{ProcessingResult, StepRunSummary}
import repcheck.pipeline.models.workflow.state.WorkflowStepStatus

/**
 * Canonical pipeline execution logic, consolidated from the nine near-identical per-pipeline copies (D-IC). Accepts a
 * pre-built result stream and logger so tests can inject stubs without constructing the full dependency graph. When a
 * [[WorkflowStateUpdater]] is provided, records step start/completion/failure in `workflow_run_steps`.
 *
 * The result stream is consumed via `compile.fold` over a bounded [[StreamingStats]] accumulator — never
 * `compile.toList` — so memory stays bounded regardless of how many events the run processes. Per-item failure detail
 * remains visible in the item-level logs each pipeline already emits with correlation IDs.
 *
 * Semantics, unified across all pipelines:
 *   - Skipped rolls into succeeded (an idempotent re-delivery is a healthy no-op, not a separate bucket)
 *   - empty stream -> Completed; all failed -> Failed; some failed -> CompletedWithErrors; none failed -> Completed
 *   - exit code: Success iff itemsFailed == 0
 *   - logging: one summary line; one extra failure-reasons line (reason -> count) only when failures occurred
 */
object PipelineExecutor {

  /**
   * @param runId
   *   workflow-run identifier from the IOApp's CLI args, as the String the [[WorkflowStateUpdater]] key expects. Pass
   *   "0" when no workflow registrar is in scope.
   * @param stepRunId
   *   workflow_run_steps row identifier, stored on the [[StepRunSummary]]. Defaults to `0L` — placeholder used by
   *   pipelines that do not wire that table yet.
   */
  def execute[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    pipelineName: String,
    runId: String,
    stepRunId: Long = 0L,
    workflowStateUpdater: Option[WorkflowStateUpdater[F]] = None,
  ): F[ExitCode] = {
    val logCtx = LogContext(runId = runId, stepName = pipelineName)

    for {
      _           <- recordStepStarted(workflowStateUpdater, runId, pipelineName)
      startedAt   <- Async[F].realTimeInstant
      stats       <- accumulateResults(resultStream, logger, logCtx)
      completedAt <- Async[F].realTimeInstant
      summary = buildSummary(stats, stepRunId, pipelineName, startedAt, completedAt)
      _ <- logSummary(logger, logCtx, summary)
      exitCode =
        if (summary.itemsFailed == 0) { ExitCode.Success }
        else { ExitCode.Error }
      _ <- recordStepOutcome(workflowStateUpdater, runId, pipelineName, summary)
    } yield exitCode
  }

  private def accumulateResults[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    logCtx: LogContext,
  ): F[StreamingStats] =
    resultStream.compile.fold(StreamingStats.empty)(_.add(_)).handleErrorWith { error =>
      logger.error(logCtx, s"Stream failed: ${error.getMessage}", Some(error)) *>
        Async[F].raiseError(error)
    }

  private[execution] def buildSummary(
    stats: StreamingStats,
    stepRunId: Long,
    pipelineName: String,
    startedAt: java.time.Instant,
    completedAt: java.time.Instant,
  ): StepRunSummary = {
    val status: WorkflowStepStatus =
      if (stats.itemsProcessed == 0) {
        WorkflowStepStatus.Completed
      } else if (stats.itemsFailed == stats.itemsProcessed) {
        WorkflowStepStatus.Failed
      } else if (stats.itemsFailed > 0) {
        WorkflowStepStatus.CompletedWithErrors
      } else {
        WorkflowStepStatus.Completed
      }

    StepRunSummary(
      stepRunId = stepRunId,
      stepName = pipelineName,
      status = status,
      startedAt = startedAt,
      completedAt = completedAt,
      itemsProcessed = stats.itemsProcessed,
      itemsSucceeded = stats.itemsSucceeded,
      itemsFailed = stats.itemsFailed,
      errorCounts = stats.errorCounts,
    )
  }

  private def logSummary[F[_]: Async](
    logger: PipelineLogger[F],
    logCtx: LogContext,
    summary: StepRunSummary,
  ): F[Unit] = {
    val headline = logger.info(
      logCtx,
      s"Pipeline completed: ${summary.itemsProcessed.toString} processed, " +
        s"${summary.itemsSucceeded.toString} succeeded, ${summary.itemsFailed.toString} failed",
    )
    if (summary.itemsFailed > 0) {
      headline *> logger.warn(logCtx, s"Failure reasons: ${formatErrorCounts(summary.errorCounts)}")
    } else {
      headline
    }
  }

  private[execution] def formatErrorCounts(errorCounts: Map[String, Int]): String =
    errorCounts.toList
      .sortBy { case (reason, count) => (-count, reason) }
      .map { case (reason, count) => s"$reason x$count" }
      .mkString("; ")

  private def recordStepStarted[F[_]: Async](
    updater: Option[WorkflowStateUpdater[F]],
    runId: String,
    stepName: String,
  ): F[Unit] =
    updater.traverse_(_.recordStepStarted(runId, stepName))

  private def recordStepOutcome[F[_]: Async](
    updater: Option[WorkflowStateUpdater[F]],
    runId: String,
    stepName: String,
    summary: StepRunSummary,
  ): F[Unit] =
    updater.traverse_ { wsu =>
      if (summary.itemsFailed == 0) {
        wsu.recordStepCompleted(runId, stepName)
      } else {
        wsu.recordStepFailed(
          runId,
          stepName,
          s"${summary.itemsFailed} of ${summary.itemsProcessed} items failed",
        )
      }
    }

}
