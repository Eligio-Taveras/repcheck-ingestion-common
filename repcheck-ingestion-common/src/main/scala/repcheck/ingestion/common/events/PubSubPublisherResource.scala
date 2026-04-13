package repcheck.ingestion.common.events

import java.util.concurrent.TimeUnit

import cats.effect.{Resource, Sync}

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.TopicName

import io.grpc.ManagedChannelBuilder

object PubSubPublisherResource {

  private[events] def defaultPublisherFactory[F[_]: Sync](
    config: EventPublisherConfig
  ): F[Publisher] =
    Sync[F].blocking {
      val topicName = TopicName.of(config.projectId, config.topicName)
      val builder   = Publisher.newBuilder(topicName)

      // When the Pub/Sub emulator is configured, use no-auth and connect directly
      sys.env.get("PUBSUB_EMULATOR_HOST").foreach { emulatorHost =>
        val channel         = ManagedChannelBuilder.forTarget(emulatorHost).usePlaintext().build()
        val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
        builder.setChannelProvider(channelProvider)
        builder.setCredentialsProvider(NoCredentialsProvider.create())
      }

      builder.build()
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
