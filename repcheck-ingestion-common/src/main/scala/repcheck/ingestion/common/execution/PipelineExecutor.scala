package repcheck.ingestion.common.execution

import cats.effect.{Async, ExitCode}
import cats.syntax.all._

import fs2.Stream

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.{ProcessingResult, StepRunSummary}
import repcheck.pipeline.models.workflow.state.WorkflowStepStatus

/**
 * Canonical pipeline execution, replacing the nine per-pipeline copies. Consumes the result stream via `compile.fold` —
 * never `compile.toList` — so memory stays bounded however many events the run processes; per-item failure detail lives
 * in the item-level logs, not here.
 */
object PipelineExecutor {

  def execute[F[_]: Async](
    resultStream: Stream[F, ProcessingResult],
    logger: PipelineLogger[F],
    pipelineName: String,
    runId: Long,
    stepRunId: Long = 0L,
    workflowStateUpdater: Option[WorkflowStateUpdater[F]] = None,
  ): F[ExitCode] = {
    val logCtx = LogContext(runId = runId.toString, stepName = pipelineName)

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
    runId: Long,
    stepName: String,
  ): F[Unit] =
    updater.traverse_(_.recordStepStarted(runId, stepName))

  private def recordStepOutcome[F[_]: Async](
    updater: Option[WorkflowStateUpdater[F]],
    runId: Long,
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
