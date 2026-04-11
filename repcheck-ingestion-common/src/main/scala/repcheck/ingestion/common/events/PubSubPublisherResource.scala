package repcheck.ingestion.common.events

import java.util.concurrent.TimeUnit

import cats.effect.{Resource, Sync}

import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.TopicName

object PubSubPublisherResource {

  def make[F[_]: Sync](config: EventPublisherConfig): Resource[F, PubSubEventPublisher[F]] =
    Resource
      .make(
        Sync[F].blocking {
          val topicName = TopicName.of(config.projectId, config.topicName)
          Publisher.newBuilder(topicName).build()
        }
      )(publisher =>
        Sync[F].blocking {
          publisher.shutdown()
          val _ = publisher.awaitTermination(30, TimeUnit.SECONDS)
        }
      )
      .map(sdkPublisher => new GooglePubSubEventPublisher[F](sdkPublisher))

}
