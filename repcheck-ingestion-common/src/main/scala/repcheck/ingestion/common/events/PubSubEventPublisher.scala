package repcheck.ingestion.common.events

trait PubSubEventPublisher[F[_]] {
  def publish(topic: String, data: String, attributes: Map[String, String]): F[String]
}
