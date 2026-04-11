package repcheck.ingestion.common.events

import java.util.concurrent.TimeUnit

import cats.effect.{Resource, Sync}

import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.TopicName

object PubSubPublisherResource {

  private[events] def defaultPublisherFactory[F[_]: Sync](
    config: EventPublisherConfig
  ): F[Publisher] =
    Sync[F].blocking {
      val topicName = TopicName.of(config.projectId, config.topicName)
      Publisher.newBuilder(topicName).build()
    }

  def make[F[_]: Sync](
    publisherFactory: F[Publisher]
  ): Resource[F, PubSubEventPublisher[F]] =
    Resource
      .make(publisherFactory)(publisher =>
        Sync[F].blocking {
          publisher.shutdown()
          val _ = publisher.awaitTermination(30, TimeUnit.SECONDS)
        }
      )
      .map(sdkPublisher => new GooglePubSubEventPublisher[F](sdkPublisher))

  def make[F[_]: Sync](config: EventPublisherConfig): Resource[F, PubSubEventPublisher[F]] =
    make(defaultPublisherFactory(config))

}
