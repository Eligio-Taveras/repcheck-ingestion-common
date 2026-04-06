package repcheck.ingestion.common.events

import java.util.UUID

import repcheck.pipeline.models.events.{
  BillTextAvailableEvent,
  BillTextIngestedEvent,
  MemberUpdatedEvent,
  VoteRecordedEvent,
}

trait IngestionEventPublisher[F[_]] {
  def billTextAvailable(event: BillTextAvailableEvent, correlationId: UUID): F[String]
  def billTextIngested(event: BillTextIngestedEvent, correlationId: UUID): F[String]
  def voteRecorded(event: VoteRecordedEvent, correlationId: UUID): F[String]
  def memberUpdated(event: MemberUpdatedEvent, correlationId: UUID): F[String]
}
