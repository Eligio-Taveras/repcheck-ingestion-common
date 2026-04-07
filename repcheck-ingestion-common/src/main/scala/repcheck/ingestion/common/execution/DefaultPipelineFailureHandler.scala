package repcheck.ingestion.common.execution

import cats.Monad
import cats.syntax.all._

import repcheck.ingestion.common.events.PubSubEventPublisher

/**
 * Default [[PipelineFailureHandler]] implementation.
 *
 *   1. Increment the step's retry count via the [[WorkflowStateUpdater]].
 *   1. If the new count is `<= config.maxRetries`, republish `originalMessage` to the same Pub/Sub topic and return
 *      [[FailureAction.Requeued]].
 *   1. Otherwise, record the step as failed with the error's message and return [[FailureAction.PermanentlyFailed]].
 *
 * The original Pub/Sub attributes are not carried across the requeue — the Launcher and downstream consumers rely on
 * the envelope inside the payload itself. The `retryCount` attribute is added so observers can distinguish retries from
 * first-attempt deliveries.
 */
class DefaultPipelineFailureHandler[F[_]: Monad](
  stateUpdater: WorkflowStateUpdater[F],
  publisher: PubSubEventPublisher[F],
  topic: String,
  config: PipelineFailureHandlerConfig,
) extends PipelineFailureHandler[F] {

  override def handleFailure(
    runId: String,
    stepName: String,
    originalMessage: String,
    error: Throwable,
  ): F[FailureAction] =
    stateUpdater.incrementRetryCount(runId, stepName).flatMap { newCount =>
      if (newCount <= config.maxRetries) {
        val attrs = Map("retryCount" -> newCount.toString)
        publisher.publish(topic, originalMessage, attrs).as(FailureAction.Requeued(newCount))
      } else {
        val message = Option(error.getMessage).getOrElse(error.getClass.getSimpleName)
        stateUpdater
          .recordStepFailed(runId, stepName, message)
          .as(FailureAction.PermanentlyFailed(newCount))
      }
    }

}
