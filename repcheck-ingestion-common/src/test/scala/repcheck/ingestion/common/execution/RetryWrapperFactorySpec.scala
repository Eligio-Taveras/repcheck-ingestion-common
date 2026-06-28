package repcheck.ingestion.common.execution

import java.util.UUID

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref}

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

import com.repcheck.utils.errors.{ErrorClass, ErrorClassifier, RetryConfig}

class RetryWrapperFactorySpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  /** Captures (level, context, message) per log call so retry callbacks can be asserted on. */
  private def capturingLogger(sink: Ref[IO, Vector[(String, LogContext, String)]]): PipelineLogger[IO] =
    new PipelineLogger[IO] {
      def debug(ctx: LogContext, m: String): IO[Unit] = sink.update(_ :+ (("debug", ctx, m)))
      def info(ctx: LogContext, m: String): IO[Unit]  = sink.update(_ :+ (("info", ctx, m)))
      def warn(ctx: LogContext, m: String): IO[Unit]  = sink.update(_ :+ (("warn", ctx, m)))
      def error(ctx: LogContext, m: String, cause: Option[Throwable]): IO[Unit] =
        sink.update(_ :+ (("error", ctx, m)))
    }

  // Treat every error as Transient so withRetry actually retries.
  private val alwaysTransient: ErrorClassifier = (_: Throwable) => ErrorClass.Transient

  // Tiny backoff so a single retry resolves near-instantly.
  private val fastConfig =
    RetryConfig(maxRetries = 3, initialBackoffMs = 1L, maxBackoffMs = 5L, backoffMultiplier = 2.0)

  private val correlationId = UUID.fromString("00000000-0000-0000-0000-0000000000aa")

  "logging" should "emit a WARN tagged with the correlationId when a retry is scheduled" in {
    val test = for {
      sink <- Ref.of[IO, Vector[(String, LogContext, String)]](Vector.empty)
      wrapper = RetryWrapperFactory.logging[IO](capturingLogger(sink), "test-pipeline")
      attempt <- Ref.of[IO, Int](0)
      // Fail the first attempt, succeed on the second — exactly one retry callback fires.
      action = attempt.getAndUpdate(_ + 1).flatMap { n =>
        if (n == 0) IO.raiseError(new RuntimeException("transient boom")) else IO.pure("ok")
      }
      result <- wrapper.withRetry(action, fastConfig, alwaysTransient, (_, t) => t, correlationId)
      logged <- sink.get
    } yield {
      val _                 = result shouldBe "ok"
      val warns             = logged.filter(_._1 == "warn")
      val _                 = warns should have size 1
      val (_, ctx, message) = warns.headOption.getOrElse(fail("expected a WARN line"))
      val _                 = ctx.correlationId shouldBe Some(correlationId)
      val _                 = ctx.stepName shouldBe "test-pipeline"
      message should include("Retry 1/3")
    }
    test
  }

  "noOp" should "retry silently (callback is a no-op) and still surface the eventual success" in {
    val test = for {
      wrapper <- IO.pure(RetryWrapperFactory.noOp[IO])
      attempt <- Ref.of[IO, Int](0)
      action = attempt.getAndUpdate(_ + 1).flatMap { n =>
        if (n == 0) IO.raiseError(new RuntimeException("transient boom")) else IO.pure("ok")
      }
      result <- wrapper.withRetry(action, fastConfig, alwaysTransient, (_, t) => t, correlationId)
      calls  <- attempt.get
    } yield {
      val _ = result shouldBe "ok"
      // 1 failure + 1 success == the action ran twice, proving the retry path (with its no-op callback) executed.
      calls shouldBe 2
    }
    test
  }

}
