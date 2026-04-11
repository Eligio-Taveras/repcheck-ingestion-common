package repcheck.ingestion.common.events

final case class EventPublishFailed(
  topic: String,
  detail: String,
  cause: Option[Throwable] = None,
) extends Exception(s"Failed to publish event to $topic: $detail") {
  cause.foreach(initCause)
}
