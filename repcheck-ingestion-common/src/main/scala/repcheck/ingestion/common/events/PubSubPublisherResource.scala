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

  /** Configure the Publisher builder for the Pub/Sub emulator when PUBSUB_EMULATOR_HOST is set. */
  private[events] def configureEmulator(
    builder: Publisher.Builder,
    emulatorHost: Option[String],
  ): Publisher.Builder = {
    emulatorHost.foreach { host =>
      val channel         = ManagedChannelBuilder.forTarget(host).usePlaintext().build()
      val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel))
      builder.setChannelProvider(channelProvider)
      builder.setCredentialsProvider(NoCredentialsProvider.create())
    }
    builder
  }

  private[events] def defaultPublisherFactory[F[_]: Sync](
    config: EventPublisherConfig
  ): F[Publisher] =
    Sync[F].blocking {
      val topicName = TopicName.of(config.projectId, config.topicName)
      val builder   = configureEmulator(Publisher.newBuilder(topicName), sys.env.get("PUBSUB_EMULATOR_HOST"))
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
