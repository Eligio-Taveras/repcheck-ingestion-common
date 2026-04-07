package repcheck.ingestion.common.execution

/**
 * Encapsulates the retry-or-fail decision per Component 2 §2.3.
 *
 * On failure, the handler first increments the step's retry count in the workflow state. If the new count is within the
 * configured `maxRetries`, it republishes the original Pub/Sub message to the same topic and returns
 * [[FailureAction.Requeued]] — the current execution stops and a brand new one will pick up the redelivered message.
 * Otherwise it records the step as failed and returns [[FailureAction.PermanentlyFailed]].
 */
trait PipelineFailureHandler[F[_]] {

  def handleFailure(
    runId: String,
    stepName: String,
    originalMessage: String,
    error: Throwable,
  ): F[FailureAction]

}
