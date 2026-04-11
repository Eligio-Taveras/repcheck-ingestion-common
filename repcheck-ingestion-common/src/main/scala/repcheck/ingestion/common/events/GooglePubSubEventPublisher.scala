package repcheck.ingestion.common.events

import cats.effect.Sync
import cats.syntax.all._

import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.PubsubMessage

class GooglePubSubEventPublisher[F[_]: Sync](
  publisher: Publisher
) extends PubSubEventPublisher[F] {

  override def publish(topic: String, data: String, attributes: Map[String, String]): F[String] =
    Sync[F]
      .blocking {
        val messageBuilder = PubsubMessage
          .newBuilder()
          .setData(ByteString.copyFromUtf8(data))

        attributes.foreach { case (k, v) => messageBuilder.putAttributes(k, v) }

        val message   = messageBuilder.build()
        val apiFuture = publisher.publish(message)
        apiFuture.get()
      }
      .adaptError {
        case e: Exception =>
          EventPublishFailed(topic, e.getMessage, Some(e))
      }

}
