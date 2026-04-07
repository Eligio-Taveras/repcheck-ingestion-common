package repcheck.ingestion.common.execution

/**
 * Outcome of a pipeline failure handling decision.
 *
 *   - [[Requeued]] — the original Pub/Sub message was republished with an incremented retry count. The current
 *     execution should stop; a new one will begin when the message is redelivered.
 *   - [[PermanentlyFailed]] — the step has exceeded its retry budget and has been marked as `Failed` in the workflow
 *     state. No further automatic retries will occur.
 */
sealed trait FailureAction {
  def retryCount: Int
}

object FailureAction {
  final case class Requeued(retryCount: Int)          extends FailureAction
  final case class PermanentlyFailed(retryCount: Int) extends FailureAction
}
