package repcheck.ingestion.common.events

import java.util.UUID

import cats.effect.Sync
import cats.syntax.all._

import io.circe.Encoder
import io.circe.syntax._

import repcheck.pipeline.models.events.{
  BillTextAvailableEvent,
  BillTextIngestedEvent,
  EventTypes,
  MemberUpdatedEvent,
  PipelineEvent,
  VoteRecordedEvent,
}

class DefaultIngestionEventPublisher[F[_]: Sync](
  publisher: PubSubEventPublisher[F],
  topicName: String,
  source: String,
) extends IngestionEventPublisher[F] {

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
  ): F[String] =
    for {
      envelope <- PipelineEvent.create[F, T](eventType, payload, correlationId, source)
      json = envelope.asJson(using PipelineEvent.encoder[T]).noSpaces
      messageId <- publisher.publish(topicName, json, Map("eventType" -> eventType))
    } yield messageId

}
