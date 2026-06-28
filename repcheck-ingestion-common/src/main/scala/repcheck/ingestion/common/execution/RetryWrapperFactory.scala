package repcheck.ingestion.common.execution

import cats.Applicative
import cats.effect.Temporal

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

import com.repcheck.utils.errors.RetryWrapper

/**
 * Builds the two `RetryWrapper[F]` shapes the pipelines use, replacing the per-app copies (`buildRetryWrapper` in the
 * amendments and bill-metadata pipelines; the no-op variant in the bill-text availability checker).
 *
 * `RetryWrapper`'s constructor requires a `cats.effect.Temporal[F]` (it schedules the back-off sleeps), so both factory
 * methods carry that constraint — `Applicative` alone is not enough to construct one.
 */
object RetryWrapperFactory {

  /**
   * A wrapper whose on-retry callback funnels every attempt into the pipeline's structured logger as a WARN, tagging
   * the line with the failing item's correlation id. Without this the wrapper is a black hole on retries and a stalled
   * timeout looks like a frozen pipeline to operators.
   */
  def logging[F[_]: Temporal](logger: PipelineLogger[F], pipelineName: String): RetryWrapper[F] = {
    val ctx = LogContext("0", pipelineName)
    new RetryWrapper[F]((attempt, maxRetries, delayMs, errorClass, message, correlationId) =>
      logger.warn(
        ctx.copy(correlationId = Some(correlationId)),
        s"Retry ${attempt.toString}/${maxRetries.toString} scheduled in ${delayMs.toString}ms " +
          s"(errorClass=${errorClass.toString}): $message",
      )
    )
  }

  /** A wrapper whose on-retry callback does nothing — for pipelines that do not surface retry attempts. */
  def noOp[F[_]: Temporal]: RetryWrapper[F] =
    new RetryWrapper[F]((_, _, _, _, _, _) => Applicative[F].unit)

}
