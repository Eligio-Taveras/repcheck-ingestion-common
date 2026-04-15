package repcheck.ingestion.common.events

import java.time.Instant
import java.util.UUID

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}

import io.circe.parser.decode

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.{RetryConfig, RetryWrapper}
import repcheck.pipeline.models.events.{
  BillTextAvailableEvent,
  BillTextIngestedEvent,
  MemberUpdatedEvent,
  PipelineEvent,
  VoteRecordedEvent,
}

class IngestionEventPublisherSpec extends AnyFlatSpec with Matchers {

  private val topicName     = "projects/test/topics/ingestion-events"
  private val source        = "test-pipeline"
  private val noRetryConfig = RetryConfig(maxRetries = 0)

  private val noOpRetryWrapper: RetryWrapper[IO] =
    new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)

  private def createCapturingPublisher: IO[(CapturingPublisher[IO], DefaultIngestionEventPublisher[IO])] =
    for {
      ref <- Ref.of[IO, List[(String, String, Map[String, String])]](List.empty)
      capturingPub = new CapturingPublisher[IO](ref)
      eventPub = new DefaultIngestionEventPublisher[IO](
        capturingPub,
        topicName,
        source,
        noOpRetryWrapper,
        noRetryConfig,
      )
    } yield (capturingPub, eventPub)

  "billTextAvailable" should "publish with eventType bill.text.available" in {
    val test = for {
      (capturingPub, eventPub) <- createCapturingPublisher
      correlationId = UUID.randomUUID()
      event = BillTextAvailableEvent(
        naturalKey = "hr1-118",
        congress = 118,
        textUrl = "https://example.com/hr1.xml",
        textFormat = "xml",
        versionCode = "ih",
        previousVersionCode = None,
      )
      msgId    <- eventPub.billTextAvailable(event, correlationId)
      captured <- capturingPub.captured
    } yield {
      val _                       = msgId shouldBe "msg-1"
      val _                       = captured should have length 1
      val (pubTopic, json, attrs) = captured.headOption.getOrElse(fail("No captured messages"))
      val _                       = pubTopic shouldBe topicName
      val _                       = json should include("\"eventType\":\"bill.text.available\"")
      val _                       = json should include("\"naturalKey\":\"hr1-118\"")
      attrs shouldBe Map("eventType" -> "bill.text.available")
    }
    test.unsafeRunSync()
  }

  "billTextIngested" should "publish with eventType bill.text.ingested" in {
    val test = for {
      (capturingPub, eventPub) <- createCapturingPublisher
      correlationId = UUID.randomUUID()
      versionId     = UUID.randomUUID()
      event = BillTextIngestedEvent(
        naturalKey = "s200-118",
        versionId = versionId,
        congress = 118,
        versionCode = "enr",
        previousVersionCode = Some("eh"),
      )
      msgId    <- eventPub.billTextIngested(event, correlationId)
      captured <- capturingPub.captured
    } yield {
      val _                = msgId shouldBe "msg-1"
      val (_, json, attrs) = captured.headOption.getOrElse(fail("No captured messages"))
      val _                = json should include("\"eventType\":\"bill.text.ingested\"")
      val _                = json should include("\"naturalKey\":\"s200-118\"")
      attrs shouldBe Map("eventType" -> "bill.text.ingested")
    }
    test.unsafeRunSync()
  }

  "voteRecorded" should "publish with eventType vote.recorded" in {
    val test = for {
      (capturingPub, eventPub) <- createCapturingPublisher
      correlationId = UUID.randomUUID()
      event = VoteRecordedEvent(
        voteId = "h123-118.2024",
        naturalKey = Some("hr1-118"),
        chamber = "House",
        date = Instant.parse("2024-03-15T14:30:00Z"),
        congress = 118,
        isUpdate = false,
      )
      msgId    <- eventPub.voteRecorded(event, correlationId)
      captured <- capturingPub.captured
    } yield {
      val _                = msgId shouldBe "msg-1"
      val (_, json, attrs) = captured.headOption.getOrElse(fail("No captured messages"))
      val _                = json should include("\"eventType\":\"vote.recorded\"")
      val _                = json should include("\"voteId\":\"h123-118.2024\"")
      attrs shouldBe Map("eventType" -> "vote.recorded")
    }
    test.unsafeRunSync()
  }

  "memberUpdated" should "publish with eventType member.updated" in {
    val test = for {
      (capturingPub, eventPub) <- createCapturingPublisher
      correlationId = UUID.randomUUID()
      event         = MemberUpdatedEvent(memberId = "A000001")
      msgId    <- eventPub.memberUpdated(event, correlationId)
      captured <- capturingPub.captured
    } yield {
      val _                = msgId shouldBe "msg-1"
      val (_, json, attrs) = captured.headOption.getOrElse(fail("No captured messages"))
      val _                = json should include("\"eventType\":\"member.updated\"")
      val _                = json should include("\"memberId\":\"A000001\"")
      attrs shouldBe Map("eventType" -> "member.updated")
    }
    test.unsafeRunSync()
  }

  "source string" should "come from config — different source configs produce different source in events" in {
    val test = for {
      ref1 <- Ref.of[IO, List[(String, String, Map[String, String])]](List.empty)
      ref2 <- Ref.of[IO, List[(String, String, Map[String, String])]](List.empty)
      pub1 = new CapturingPublisher[IO](ref1)
      pub2 = new CapturingPublisher[IO](ref2)
      eventPub1 = new DefaultIngestionEventPublisher[IO](
        pub1,
        topicName,
        "bills-pipeline",
        noOpRetryWrapper,
        noRetryConfig,
      )
      eventPub2 = new DefaultIngestionEventPublisher[IO](
        pub2,
        topicName,
        "votes-pipeline",
        noOpRetryWrapper,
        noRetryConfig,
      )
      correlationId = UUID.randomUUID()
      event         = MemberUpdatedEvent(memberId = "B000001")
      _         <- eventPub1.memberUpdated(event, correlationId)
      _         <- eventPub2.memberUpdated(event, correlationId)
      captured1 <- pub1.captured
      captured2 <- pub2.captured
    } yield {
      val (_, json1, _) = captured1.headOption.getOrElse(fail("No captured messages from pub1"))
      val (_, json2, _) = captured2.headOption.getOrElse(fail("No captured messages from pub2"))
      val _             = json1 should include("\"source\":\"bills-pipeline\"")
      json2 should include("\"source\":\"votes-pipeline\"")
    }
    test.unsafeRunSync()
  }

  "event envelope" should "include generated UUID and timestamp" in {
    val beforeTest = Instant.now()
    val test = for {
      (capturingPub, eventPub) <- createCapturingPublisher
      correlationId = UUID.randomUUID()
      event         = MemberUpdatedEvent(memberId = "C000001")
      _        <- eventPub.memberUpdated(event, correlationId)
      captured <- capturingPub.captured
    } yield {
      val (_, json, _) = captured.headOption.getOrElse(fail("No captured messages"))
      val parsed       = decode[PipelineEvent[MemberUpdatedEvent]](json)
      val _            = parsed.isRight shouldBe true
      val envelope     = parsed.getOrElse(fail("Failed to decode PipelineEvent"))
      val _            = envelope.eventId.toString should not be empty
      val _            = envelope.correlationId shouldBe correlationId
      val _            = envelope.timestamp.toString should not be empty
      val afterTest    = Instant.now()
      val _            = envelope.timestamp.isAfter(beforeTest.minusSeconds(1)) shouldBe true
      envelope.timestamp.isBefore(afterTest.plusSeconds(1)) shouldBe true
    }
    test.unsafeRunSync()
  }

  "publisher failure" should "propagate to caller as EventPublishFailed" in {
    val failingPublisher = new PubSubEventPublisher[IO] {
      override def publish(topic: String, data: String, attributes: Map[String, String]): IO[String] =
        IO.raiseError(new RuntimeException("Pub/Sub unavailable"))
    }
    val eventPub =
      new DefaultIngestionEventPublisher[IO](failingPublisher, topicName, source, noOpRetryWrapper, noRetryConfig)
    val event = MemberUpdatedEvent(memberId = "D000001")

    val result = eventPub.memberUpdated(event, UUID.randomUUID()).attempt.unsafeRunSync()
    val _      = result.isLeft shouldBe true
    val error  = result.swap.getOrElse(fail("Expected error"))
    val _      = error shouldBe a[EventPublishFailed]
    error.getMessage should include("Pub/Sub unavailable")
  }

  "attributes" should "include eventType for each method" in {
    val test = for {
      ref <- Ref.of[IO, List[(String, String, Map[String, String])]](List.empty)
      capturingPub = new CapturingPublisher[IO](ref)
      eventPub = new DefaultIngestionEventPublisher[IO](
        capturingPub,
        topicName,
        source,
        noOpRetryWrapper,
        noRetryConfig,
      )
      correlationId = UUID.randomUUID()

      _ <- eventPub.billTextAvailable(
        BillTextAvailableEvent("hr1-118", 118, "https://example.com/hr1.xml", "xml", "ih", None),
        correlationId,
      )
      _ <- eventPub.billTextIngested(
        BillTextIngestedEvent("hr1-118", UUID.randomUUID(), 118, "ih", None),
        correlationId,
      )
      _ <- eventPub.voteRecorded(
        VoteRecordedEvent("v1", Some("hr1-118"), "House", Instant.now(), 118, false),
        correlationId,
      )
      _ <- eventPub.memberUpdated(MemberUpdatedEvent("A000001"), correlationId)

      captured <- capturingPub.captured
    } yield {
      val _ = captured should have length 4
      val _ = captured(0)._3 shouldBe Map("eventType" -> "bill.text.available")
      val _ = captured(1)._3 shouldBe Map("eventType" -> "bill.text.ingested")
      val _ = captured(2)._3 shouldBe Map("eventType" -> "vote.recorded")
      captured(3)._3 shouldBe Map("eventType" -> "member.updated")
    }
    test.unsafeRunSync()
  }

  "retry" should "succeed after transient publish failure" in {
    val retryConfig  = RetryConfig(maxRetries = 3, initialBackoffMs = 1L, maxBackoffMs = 1L)
    val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)

    val test = for {
      callCount <- Ref.of[IO, Int](0)
      failThenSucceedPub = new FailThenSucceedPublisher(callCount, failUntil = 1)
      eventPub = new DefaultIngestionEventPublisher[IO](
        failThenSucceedPub,
        topicName,
        source,
        retryWrapper,
        retryConfig,
      )
      correlationId = UUID.randomUUID()
      event         = MemberUpdatedEvent(memberId = "R000001")
      msgId <- eventPub.memberUpdated(event, correlationId)
      count <- callCount.get
    } yield {
      val _ = msgId shouldBe "msg-2"
      count shouldBe 2
    }
    test.unsafeRunSync()
  }

  it should "propagate error after all retries exhausted" in {
    val retryConfig  = RetryConfig(maxRetries = 2, initialBackoffMs = 1L, maxBackoffMs = 1L)
    val retryWrapper = new RetryWrapper[IO]((_, _, _, _, _, _) => IO.unit)

    val alwaysFailPublisher = new PubSubEventPublisher[IO] {
      override def publish(topic: String, data: String, attributes: Map[String, String]): IO[String] =
        IO.raiseError(EventPublishFailed(topic, "always fails"))
    }
    val eventPub =
      new DefaultIngestionEventPublisher[IO](alwaysFailPublisher, topicName, source, retryWrapper, retryConfig)
    val event = MemberUpdatedEvent(memberId = "R000002")

    val result = eventPub.memberUpdated(event, UUID.randomUUID()).attempt.unsafeRunSync()
    val _      = result.isLeft shouldBe true
    val error  = result.swap.getOrElse(fail("Expected error"))
    val _      = error shouldBe a[EventPublishFailed]
    error.getMessage should include(topicName)
  }

}

class FailThenSucceedPublisher(ref: Ref[IO, Int], failUntil: Int) extends PubSubEventPublisher[IO] {

  override def publish(topic: String, data: String, attributes: Map[String, String]): IO[String] =
    ref.modify { count =>
      val next = count + 1
      if (next <= failUntil) {
        (next, IO.raiseError[String](EventPublishFailed(topic, s"attempt $next")))
      } else {
        (next, IO.pure(s"msg-$next"))
      }
    }.flatten

}

class CapturingPublisher[F[_]](ref: Ref[F, List[(String, String, Map[String, String])]])
    extends PubSubEventPublisher[F] {

  override def publish(topic: String, data: String, attributes: Map[String, String]): F[String] =
    ref.modify { msgs =>
      val updated = msgs :+ (topic, data, attributes)
      (updated, s"msg-${updated.length.toString}")
    }

  def captured: F[List[(String, String, Map[String, String])]] = ref.get

}
