package repcheck.ingestion.common.events

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import com.google.api.core.SettableApiFuture
import com.google.cloud.pubsub.v1.Publisher
import com.google.pubsub.v1.PubsubMessage

import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GooglePubSubEventPublisherSpec extends AnyFlatSpec with Matchers {

  private def createMockPublisher(messageId: String): Publisher = {
    val mockPub = mock(classOf[Publisher])
    val future  = SettableApiFuture.create[String]()
    future.set(messageId)
    when(mockPub.publish(any[PubsubMessage]())).thenReturn(future)
    mockPub
  }

  "GooglePubSubEventPublisher" should "publish a message and return the message ID" in {
    val mockPub   = createMockPublisher("msg-123")
    val publisher = new GooglePubSubEventPublisher[IO](mockPub)

    val result = publisher
      .publish("projects/p/topics/t", """{"key":"value"}""", Map("eventType" -> "test"))
      .unsafeRunSync()

    result shouldBe "msg-123"
  }

  it should "include data and attributes in the published message" in {
    val mockPub = mock(classOf[Publisher])
    val captor  = ArgumentCaptor.forClass(classOf[PubsubMessage])
    val future  = SettableApiFuture.create[String]()
    future.set("msg-456")
    when(mockPub.publish(captor.capture())).thenReturn(future)

    val publisher = new GooglePubSubEventPublisher[IO](mockPub)
    val _         = publisher.publish("topic", """{"hello":"world"}""", Map("key1" -> "val1")).unsafeRunSync()

    val captured = captor.getValue
    val _        = captured.getData.toStringUtf8 shouldBe """{"hello":"world"}"""
    captured.getAttributesMap.get("key1") shouldBe "val1"
  }

  it should "wrap SDK exceptions in EventPublishFailed" in {
    val mockPub = mock(classOf[Publisher])
    val future  = SettableApiFuture.create[String]()
    future.setException(new RuntimeException("connection refused"))
    when(mockPub.publish(any[PubsubMessage]())).thenReturn(future)

    val publisher = new GooglePubSubEventPublisher[IO](mockPub)

    val ex = intercept[EventPublishFailed] {
      publisher.publish("projects/p/topics/t", "data", Map.empty).unsafeRunSync()
    }
    ex.topic shouldBe "projects/p/topics/t"
  }

  it should "handle empty attributes" in {
    val mockPub   = createMockPublisher("msg-789")
    val publisher = new GooglePubSubEventPublisher[IO](mockPub)

    val result = publisher.publish("topic", "data", Map.empty).unsafeRunSync()
    result shouldBe "msg-789"
  }

  it should "handle multiple attributes" in {
    val mockPub = mock(classOf[Publisher])
    val captor  = ArgumentCaptor.forClass(classOf[PubsubMessage])
    val future  = SettableApiFuture.create[String]()
    future.set("msg-multi")
    when(mockPub.publish(captor.capture())).thenReturn(future)

    val publisher = new GooglePubSubEventPublisher[IO](mockPub)
    val attrs     = Map("eventType" -> "bill.text.available", "source" -> "checker", "retry" -> "0")
    val _         = publisher.publish("topic", "data", attrs).unsafeRunSync()

    val captured = captor.getValue.getAttributesMap
    val _        = captured.get("eventType") shouldBe "bill.text.available"
    val _        = captured.get("source") shouldBe "checker"
    captured.get("retry") shouldBe "0"
  }

}
