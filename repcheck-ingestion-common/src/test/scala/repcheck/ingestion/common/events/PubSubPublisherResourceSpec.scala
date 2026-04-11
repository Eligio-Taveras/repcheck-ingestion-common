package repcheck.ingestion.common.events

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import com.google.api.core.SettableApiFuture
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.PubsubMessage

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

  "defaultPublisherFactory" should "create a Publisher that requires shutdown" in {
    // defaultPublisherFactory creates a real SDK Publisher. We verify it succeeds
    // and produces a Publisher that can be shut down. This exercises the factory
    // code path for coverage without needing actual GCP credentials (the Publisher
    // is created in-process; it only fails on publish, not on construction).
    val publisher = PubSubPublisherResource
      .defaultPublisherFactory[IO](testConfig)
      .unsafeRunSync()

    // Publisher was created — shut it down to verify lifecycle
    publisher.shutdown()
    val terminated = publisher.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
    terminated shouldBe true
  }

  "make(config)" should "create resource using defaultPublisherFactory" in {
    // Exercises the single-arg make overload end-to-end: creates a real
    // Publisher, wraps it, then releases it on scope exit.
    val result = PubSubPublisherResource
      .make[IO](testConfig)
      .use(publisher => publisher.publish("topic", "data", Map.empty).attempt)
      .unsafeRunSync()

    // Publish will fail (no real GCP), but the resource was created and released
    result.isLeft shouldBe true
  }

}
