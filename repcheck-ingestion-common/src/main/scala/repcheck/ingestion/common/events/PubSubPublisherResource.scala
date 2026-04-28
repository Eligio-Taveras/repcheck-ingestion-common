package repcheck.ingestion.common.events

import java.util.concurrent.TimeUnit

import cats.effect.{Resource, Sync}

import com.google.api.gax.core.NoCredentialsProvider
import com.google.api.gax.grpc.GrpcTransportChannel
import com.google.api.gax.rpc.FixedTransportChannelProvider
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.TopicName

import io.grpc.{ManagedChannel, ManagedChannelBuilder}

object PubSubPublisherResource {

  /**
   * Manage the lifecycle of an emulator-targeted gRPC `ManagedChannel`.
   *
   * ==Why this exists==
   *
   * When `PUBSUB_EMULATOR_HOST` is set (local dev / docker-compose), we route the Pub/Sub publisher through a plaintext
   * gRPC channel via `FixedTransportChannelProvider`. Per gRPC's contract, channels supplied through
   * `FixedTransportChannelProvider` are NOT shut down by the consuming `Publisher` on its own `shutdown()` — the caller
   * owns the channel's lifecycle. Without explicit teardown, the channel's gRPC event-loop threads (non-daemon by
   * default) keep the JVM alive forever after the IOApp's stream completes.
   *
   * Surfaced empirically when bill-text-pipeline's container went silent for 7 hours after its Pub/Sub stream drained:
   * `docker top` showed the JVM still running with 0.12% CPU, no new processing. Resource cleanup had run (publisher
   * shutdown, stub closed) but the leaked channel kept the JVM pinned. The same pattern is latent in every pipeline
   * that uses this `make(config)` overload — it just hasn't manifested in the publisher-only pipelines yet because they
   * happen to stay alive on their own (continuously emitting). Closing this gap removes the latent failure mode.
   *
   * Returns `None` when the env var is unset (production GCP), since gRPC's default `InstantiatingGrpcChannelProvider`
   * manages its own channel lifecycle correctly — only the explicit `FixedTransportChannelProvider` path leaks.
   */
  private[events] def emulatorChannelResource[F[_]: Sync](
    emulatorHost: Option[String]
  ): Resource[F, Option[ManagedChannel]] =
    emulatorHost match {
      case None =>
        Resource.pure[F, Option[ManagedChannel]](None)
      case Some(host) =>
        Resource
          .make(
            Sync[F].blocking(ManagedChannelBuilder.forTarget(host).usePlaintext().build())
          )(channel =>
            Sync[F].blocking {
              channel.shutdown()
              val _ = channel.awaitTermination(5, TimeUnit.SECONDS)
            }
          )
          .map(Some(_))
    }

  /**
   * Configure a `Publisher.Builder` to route through an externally-supplied gRPC channel.
   *
   * The channel's lifecycle is the caller's responsibility — typically obtained from [[emulatorChannelResource]] so
   * it's tied to the same `Resource` chain that builds the publisher.
   *
   * Pre-fix (kept here for reference) this method took the emulator host string and built the channel inline, which
   * leaked the channel forever. Now the channel is a parameter and the lifecycle is explicit.
   */
  private[events] def configureEmulator(
    builder: Publisher.Builder,
    channel: Option[ManagedChannel],
  ): Publisher.Builder = {
    channel.foreach { ch =>
      val channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(ch))
      builder.setChannelProvider(channelProvider)
      builder.setCredentialsProvider(NoCredentialsProvider.create())
    }
    builder
  }

  /**
   * Construct a `Publisher` against the supplied config, optionally routing through an emulator channel.
   *
   * The channel parameter is `Option[ManagedChannel]`. When `None`, gRPC uses its default
   * `InstantiatingGrpcChannelProvider` (production GCP path). When `Some`, the publisher is wired through the supplied
   * channel via `FixedTransportChannelProvider` — typically a channel acquired from [[emulatorChannelResource]].
   */
  private[events] def defaultPublisherFactory[F[_]: Sync](
    config: EventPublisherConfig,
    channel: Option[ManagedChannel],
  ): F[Publisher] =
    Sync[F].blocking {
      val topicName = TopicName.of(config.projectId, config.topicName)
      val builder   = configureEmulator(Publisher.newBuilder(topicName), channel)
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

  /**
   * High-level entry: builds an emulator-aware channel resource (when `PUBSUB_EMULATOR_HOST` is set), then composes a
   * `Publisher` and returns it as a `PubSubEventPublisher`. Resource release order is innermost-first: publisher
   * shutdown → channel shutdown. This guarantees no leaked gRPC threads after the IOApp's stream completes — see
   * [[emulatorChannelResource]] scaladoc for the full background.
   */
  def make[F[_]: Sync](config: EventPublisherConfig): Resource[F, PubSubEventPublisher[F]] =
    for {
      channel   <- emulatorChannelResource[F](sys.env.get("PUBSUB_EMULATOR_HOST"))
      publisher <- make(defaultPublisherFactory[F](config, channel))
    } yield publisher

}
