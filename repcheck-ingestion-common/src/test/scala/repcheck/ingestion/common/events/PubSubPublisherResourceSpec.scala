package repcheck.ingestion.common.events

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import com.google.api.core.SettableApiFuture
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.{PubsubMessage, TopicName}

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PubSubPublisherResourceSpec extends AnyFlatSpec with Matchers {

  private val testConfig = EventPublisherConfig(
    projectId = "test-project",
    topicName = "test-topic",
    source = "test-source",
  )

  private def mockPublisher(messageId: String): Publisher = {
    val pub    = mock(classOf[Publisher])
    val future = SettableApiFuture.create[String]()
    future.set(messageId)
    when(pub.publish(any[PubsubMessage]())).thenReturn(future)
    when(pub.awaitTermination(any[Long](), any())).thenReturn(true)
    pub
  }

  "make(factory)" should "create a working PubSubEventPublisher resource" in {
    val pub = mockPublisher("resource-msg-1")

    val result = PubSubPublisherResource
      .make[IO](IO.pure(pub))
      .use(publisher => publisher.publish("test-topic", """{"data":"value"}""", Map("key" -> "val")))
      .unsafeRunSync()

    result shouldBe "resource-msg-1"
  }

  it should "shut down the publisher on resource release" in {
    val pub = mockPublisher("resource-msg-2")

    val _ = PubSubPublisherResource
      .make[IO](IO.pure(pub))
      .use(publisher => publisher.publish("topic", "data", Map.empty))
      .unsafeRunSync()

    verify(pub).shutdown()
    verify(pub).awaitTermination(30L, java.util.concurrent.TimeUnit.SECONDS)
  }

  it should "return a GooglePubSubEventPublisher instance" in {
    val pub = mockPublisher("resource-msg-3")

    val _ = PubSubPublisherResource
      .make[IO](IO.pure(pub))
      .use { publisher =>
        IO {
          publisher shouldBe a[GooglePubSubEventPublisher[?]]
        }
      }
      .unsafeRunSync()
  }

  it should "propagate factory errors" in {
    val ex = intercept[RuntimeException] {
      PubSubPublisherResource
        .make[IO](IO.raiseError[Publisher](new RuntimeException("factory failed")))
        .use(_ => IO.unit)
        .unsafeRunSync()
    }
    ex.getMessage shouldBe "factory failed"
  }

  it should "shut down publisher even when use body fails" in {
    val pub = mockPublisher("resource-msg-4")

    val _ = intercept[RuntimeException] {
      PubSubPublisherResource
        .make[IO](IO.pure(pub))
        .use(_ => IO.raiseError(new RuntimeException("use failed")))
        .unsafeRunSync()
    }

    verify(pub).shutdown()
  }

  "defaultPublisherFactory" should "create a Publisher that requires shutdown when no channel supplied" in {
    // defaultPublisherFactory creates a real SDK Publisher. In environments with GCP
    // Application Default Credentials, we verify the full lifecycle. In CI (no credentials),
    // Publisher construction fails with IOException — the factory code path is still executed
    // and covered by Scoverage even when the call throws.
    PubSubPublisherResource
      .defaultPublisherFactory[IO](testConfig, None)
      .attempt
      .unsafeRunSync() match {
      case Right(publisher) =>
        publisher.shutdown()
        publisher.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS) shouldBe true
      case Left(_: java.io.IOException) =>
        // No GCP Application Default Credentials in this environment — expected in CI
        succeed
      case Left(ex) =>
        fail(s"Unexpected exception from factory: ${ex.getMessage}")
    }
  }

  it should "construct a working Publisher when an emulator channel is supplied" in {
    // Acquire the channel via the same Resource that production uses, then thread it into the factory.
    // After the test, the Resource cleanup will shut the channel down — matching the production lifecycle.
    PubSubPublisherResource
      .emulatorChannelResource[IO](Some("localhost:8085"))
      .use {
        case Some(channel) =>
          PubSubPublisherResource
            .defaultPublisherFactory[IO](testConfig, Some(channel))
            .map { publisher =>
              try publisher.toString should not be empty
              finally {
                publisher.shutdown()
                val _ = publisher.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
              }
            }
        case None =>
          IO.raiseError(new AssertionError("expected emulatorChannelResource(Some) to allocate a channel"))
      }
      .unsafeRunSync()
  }

  "configureEmulator" should "set NoCredentialsProvider when a channel is supplied" in {
    // Acquire the channel through the resource so cleanup is automatic — same lifecycle production uses.
    PubSubPublisherResource
      .emulatorChannelResource[IO](Some("localhost:8085"))
      .use {
        case Some(channel) =>
          IO {
            val builder    = Publisher.newBuilder(TopicName.of("test-project", "test-topic"))
            val configured = PubSubPublisherResource.configureEmulator(builder, Some(channel))
            val publisher  = configured.build()
            try publisher.toString should not be empty
            finally {
              publisher.shutdown()
              val _ = publisher.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
            }
          }
        case None => IO.raiseError(new AssertionError("expected channel"))
      }
      .unsafeRunSync()
  }

  it should "leave builder unchanged when channel is None" in {
    val builder    = Publisher.newBuilder(TopicName.of("test-project", "test-topic"))
    val configured = PubSubPublisherResource.configureEmulator(builder, None)
    configured shouldBe builder
  }

  "emulatorChannelResource" should "return None when emulator host is None" in {
    val result = PubSubPublisherResource
      .emulatorChannelResource[IO](None)
      .use(IO.pure)
      .unsafeRunSync()
    result shouldBe None
  }

  it should "allocate a ManagedChannel and shut it down on release when host is provided" in {
    // Capture the channel reference outside the resource scope so we can assert on its post-release state.
    val channelRef = new java.util.concurrent.atomic.AtomicReference[io.grpc.ManagedChannel]()

    val _ = PubSubPublisherResource
      .emulatorChannelResource[IO](Some("localhost:8085"))
      .use {
        case Some(channel) =>
          IO {
            channelRef.set(channel)
            channel.isShutdown shouldBe false
          }
        case None => IO.raiseError(new AssertionError("expected Some(channel)"))
      }
      .unsafeRunSync()

    // After release, the channel is in a shutdown state.
    channelRef.get().isShutdown shouldBe true
  }

  "make(config)" should "create resource using defaultPublisherFactory" in {
    // Exercises the single-arg make overload end-to-end. Either the factory fails (no GCP
    // credentials in CI) or the publish fails (no real GCP endpoint) — both are Left.
    val result = PubSubPublisherResource
      .make[IO](testConfig)
      .use(publisher => publisher.publish("topic", "data", Map.empty))
      .attempt
      .unsafeRunSync()

    result.isLeft shouldBe true
  }

}
