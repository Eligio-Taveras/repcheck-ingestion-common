package repcheck.ingestion.common.execution

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}

import doobie.util.transactor.Transactor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.events.PubSubEventPublisher

class DefaultPipelineFailureHandlerSpec extends AnyFlatSpec with Matchers {

  private val topic   = "projects/test/topics/retries"
  private val runId   = "00000000-0000-0000-0000-000000000001"
  private val step    = "bills-pipeline"
  private val origMsg = """{"naturalKey":"hr1-118"}"""

  private val defaultConfig = PipelineFailureHandlerConfig(maxRetries = 3)

  /**
   * A never-connected Transactor used purely as a constructor argument for the test subclass below. All methods that
   * would touch the database are overridden in the subclass, so this Transactor is never invoked.
   */
  private val stubXa: Transactor[IO] =
    Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:stub;DB_CLOSE_DELAY=-1",
      user = "sa",
      password = "",
      logHandler = None,
    )

  /** Mutable in-memory state updater backed by a cats-effect Ref. */
  private def makeStateUpdater(
    initialRetryCount: Int
  ): IO[(Ref[IO, Int], Ref[IO, Option[String]], WorkflowStateUpdater[IO])] =
    for {
      counter <- Ref.of[IO, Int](initialRetryCount)
      failure <- Ref.of[IO, Option[String]](None)
    } yield {
      val updater = new WorkflowStateUpdater[IO](stubXa, defaultConfig) {
        override def recordStepStarted(runId: String, stepName: String): IO[Unit]   = IO.unit
        override def recordStepCompleted(runId: String, stepName: String): IO[Unit] = IO.unit
        override def recordStepFailed(runId: String, stepName: String, error: String): IO[Unit] =
          failure.set(Some(error))
        override def incrementRetryCount(runId: String, stepName: String): IO[Int] =
          counter.updateAndGet(_ + 1)
        override def getRetryCount(runId: String, stepName: String): IO[Int] =
          counter.get
      }
      (counter, failure, updater)
    }

  private def makeCapturingPublisher
    : IO[(Ref[IO, List[(String, String, Map[String, String])]], PubSubEventPublisher[IO])] =
    for {
      ref <- Ref.of[IO, List[(String, String, Map[String, String])]](List.empty)
    } yield {
      val pub = new PubSubEventPublisher[IO] {
        override def publish(t: String, d: String, a: Map[String, String]): IO[String] =
          ref.update(_ :+ ((t, d, a))).as("msg-1")
      }
      (ref, pub)
    }

  "handleFailure" should "return Requeued(1) on the first failure" in {
    val test = for {
      state <- makeStateUpdater(initialRetryCount = 0)
      (_, _, upd) = state
      pubState <- makeCapturingPublisher
      (_, pub) = pubState
      handler  = new DefaultPipelineFailureHandler[IO](upd, pub, topic, defaultConfig)
      action <- handler.handleFailure(runId, step, origMsg, new RuntimeException("boom"))
    } yield action shouldBe FailureAction.Requeued(1)
    test.unsafeRunSync()
  }

  it should "return Requeued(2) when prior retryCount was 1" in {
    val test = for {
      state <- makeStateUpdater(initialRetryCount = 1)
      (_, _, upd) = state
      pubState <- makeCapturingPublisher
      (_, pub) = pubState
      handler  = new DefaultPipelineFailureHandler[IO](upd, pub, topic, defaultConfig)
      action <- handler.handleFailure(runId, step, origMsg, new RuntimeException("boom"))
    } yield action shouldBe FailureAction.Requeued(2)
    test.unsafeRunSync()
  }

  it should "return Requeued(3) when prior retryCount was 2" in {
    val test = for {
      state <- makeStateUpdater(initialRetryCount = 2)
      (_, _, upd) = state
      pubState <- makeCapturingPublisher
      (_, pub) = pubState
      handler  = new DefaultPipelineFailureHandler[IO](upd, pub, topic, defaultConfig)
      action <- handler.handleFailure(runId, step, origMsg, new RuntimeException("boom"))
    } yield action shouldBe FailureAction.Requeued(3)
    test.unsafeRunSync()
  }

  it should "return PermanentlyFailed(4) once retryCount exceeds default max of 3" in {
    val test = for {
      state <- makeStateUpdater(initialRetryCount = 3)
      (_, failRef, upd) = state
      pubState <- makeCapturingPublisher
      (pubRef, pub) = pubState
      handler       = new DefaultPipelineFailureHandler[IO](upd, pub, topic, defaultConfig)
      action   <- handler.handleFailure(runId, step, origMsg, new RuntimeException("boom"))
      captured <- pubRef.get
      recorded <- failRef.get
    } yield {
      val _ = action shouldBe FailureAction.PermanentlyFailed(4)
      val _ = captured shouldBe empty
      recorded shouldBe Some("boom")
    }
    test.unsafeRunSync()
  }

  it should "republish the exact original message to the same topic" in {
    val test = for {
      state <- makeStateUpdater(initialRetryCount = 0)
      (_, _, upd) = state
      pubState <- makeCapturingPublisher
      (pubRef, pub) = pubState
      handler       = new DefaultPipelineFailureHandler[IO](upd, pub, topic, defaultConfig)
      _        <- handler.handleFailure(runId, step, origMsg, new RuntimeException("boom"))
      captured <- pubRef.get
    } yield {
      val _                = captured should have length 1
      val (t, data, attrs) = captured.headOption.getOrElse(fail("no message captured"))
      val _                = t shouldBe topic
      val _                = data shouldBe origMsg
      attrs shouldBe Map("retryCount" -> "1")
    }
    test.unsafeRunSync()
  }

  it should "respect a custom maxRetries configuration" in {
    val test = for {
      state <- makeStateUpdater(initialRetryCount = 5)
      (_, _, upd) = state
      pubState <- makeCapturingPublisher
      (_, pub) = pubState
      handler  = new DefaultPipelineFailureHandler[IO](upd, pub, topic, PipelineFailureHandlerConfig(maxRetries = 5))
      // 6th failure pushes the count to 6, which exceeds maxRetries=5.
      action <- handler.handleFailure(runId, step, origMsg, new RuntimeException("boom"))
    } yield action shouldBe FailureAction.PermanentlyFailed(6)
    test.unsafeRunSync()
  }

  it should "allow republish when the new count equals maxRetries exactly" in {
    val test = for {
      state <- makeStateUpdater(initialRetryCount = 4)
      (_, _, upd) = state
      pubState <- makeCapturingPublisher
      (_, pub) = pubState
      handler  = new DefaultPipelineFailureHandler[IO](upd, pub, topic, PipelineFailureHandlerConfig(maxRetries = 5))
      action <- handler.handleFailure(runId, step, origMsg, new RuntimeException("boom"))
    } yield action shouldBe FailureAction.Requeued(5)
    test.unsafeRunSync()
  }

  it should "fall back to the exception class name when getMessage is null" in {
    val test = for {
      state <- makeStateUpdater(initialRetryCount = 3)
      (_, failRef, upd) = state
      pubState <- makeCapturingPublisher
      (_, pub) = pubState
      handler  = new DefaultPipelineFailureHandler[IO](upd, pub, topic, defaultConfig)
      // A no-arg RuntimeException has a null message — handler should fall back to class name.
      action   <- handler.handleFailure(runId, step, origMsg, new RuntimeException())
      recorded <- failRef.get
    } yield {
      val _ = action shouldBe FailureAction.PermanentlyFailed(4)
      recorded shouldBe Some("RuntimeException")
    }
    test.unsafeRunSync()
  }

}
