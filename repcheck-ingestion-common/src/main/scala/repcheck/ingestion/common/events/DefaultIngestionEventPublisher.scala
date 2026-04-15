package repcheck.ingestion.common.events

import java.util.UUID

import cats.effect.Async
import cats.syntax.all._

import io.circe.Encoder
import io.circe.syntax._

import repcheck.pipeline.models.errors.{ErrorClass, ErrorClassifier, RetryConfig, RetryWrapper}
import repcheck.pipeline.models.events.{
  BillTextAvailableEvent,
  BillTextIngestedEvent,
  EventTypes,
  MemberUpdatedEvent,
  PipelineEvent,
  VoteRecordedEvent,
}

class DefaultIngestionEventPublisher[F[_]: Async](
  publisher: PubSubEventPublisher[F],
  topicName: String,
  source: String,
  retryWrapper: RetryWrapper[F],
  retryConfig: RetryConfig,
) extends IngestionEventPublisher[F] {

  private val classifier: ErrorClassifier = new ErrorClassifier {
    def classify(error: Throwable): ErrorClass = ErrorClass.Transient
  }

  override def billTextAvailable(event: BillTextAvailableEvent, correlationId: UUID): F[String] =
    publishEvent(EventTypes.BillTextAvailable, event, correlationId)

  override def billTextIngested(event: BillTextIngestedEvent, correlationId: UUID): F[String] =
    publishEvent(EventTypes.BillTextIngested, event, correlationId)

  override def voteRecorded(event: VoteRecordedEvent, correlationId: UUID): F[String] =
    publishEvent(EventTypes.VoteRecorded, event, correlationId)

  override def memberUpdated(event: MemberUpdatedEvent, correlationId: UUID): F[String] =
    publishEvent(EventTypes.MemberUpdated, event, correlationId)

  private def publishEvent[T: Encoder](
    eventType: String,
    payload: T,
    correlationId: UUID,
  ): F[String] = {
    val operation = for {
      envelope <- PipelineEvent.create[F, T](eventType, payload, correlationId, source)
      json = envelope.asJson(using PipelineEvent.encoder[T]).noSpaces
      messageId <- publisher.publish(topicName, json, Map("eventType" -> eventType))
    } yield messageId

    retryWrapper.withRetry(
      operation = operation,
      config = retryConfig,
      classifier = classifier,
      errorFactory = (msg, cause) => EventPublishFailed(topicName, msg, Some(cause)),
      correlationId = correlationId,
    )
  }

}
