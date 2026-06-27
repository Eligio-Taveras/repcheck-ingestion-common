package repcheck.ingestion.common.execution

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{ExitCode, IO, Ref}

import fs2.Stream

import doobie.util.transactor.Transactor

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.metadata.ProcessingResult
import repcheck.pipeline.models.workflow.state.WorkflowStepStatus

class PipelineExecutorSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  private def capturingLogger(messages: Ref[IO, Vector[(String, String)]]): PipelineLogger[IO] =
    new PipelineLogger[IO] {
      def debug(context: LogContext, message: String): IO[Unit] = messages.update(_ :+ (("debug", message)))
      def info(context: LogContext, message: String): IO[Unit]  = messages.update(_ :+ (("info", message)))
      def warn(context: LogContext, message: String): IO[Unit]  = messages.update(_ :+ (("warn", message)))
      def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] =
        messages.update(_ :+ (("error", message)))
    }

  /** Overrides every method, so the never-connected transactor is inert. */
  private def capturingUpdater(calls: Ref[IO, Vector[String]]): WorkflowStateUpdater[IO] = {
    val inertXa = Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = "jdbc:h2:mem:never-used",
      logHandler = None,
    )
    new WorkflowStateUpdater[IO](inertXa, PipelineFailureHandlerConfig(maxRetries = 1)) {
      override def recordStepStarted(runId: Long, stepName: String): IO[Unit] =
        calls.update(_ :+ s"started:$runId:$stepName")
      override def recordStepCompleted(runId: Long, stepName: String): IO[Unit] =
        calls.update(_ :+ s"completed:$runId:$stepName")
      override def recordStepFailed(runId: Long, stepName: String, error: String): IO[Unit] =
        calls.update(_ :+ s"failed:$runId:$stepName:$error")
    }
  }

  private def run(
    results: List[ProcessingResult],
    updater: Option[WorkflowStateUpdater[IO]] = None,
  ): IO[(ExitCode, Vector[(String, String)])] =
    for {
      messages <- Ref[IO].of(Vector.empty[(String, String)])
      code <- PipelineExecutor.execute[IO](
        Stream.emits(results).covary[IO],
        capturingLogger(messages),
        "test-pipeline",
        runId = 7L,
        stepRunId = 42L,
        workflowStateUpdater = updater,
      )
      logged <- messages.get
    } yield (code, logged)

  "execute" should "exit Success with one summary line when everything succeeds" in {
    run(List(ProcessingResult.Succeeded("a"), ProcessingResult.Succeeded("b"))).asserting {
      case (code, logged) =>
        val _ = code shouldBe ExitCode.Success
        logged shouldBe Vector(("info", "Pipeline completed: 2 processed, 2 succeeded, 0 failed"))
    }
  }

  it should "roll Skipped into succeeded" in {
    run(List(ProcessingResult.Succeeded("a"), ProcessingResult.Skipped("b", "already ingested"))).asserting {
      case (code, logged) =>
        val _ = code shouldBe ExitCode.Success
        logged.headOption.map(_._2) shouldBe Some("Pipeline completed: 2 processed, 2 succeeded, 0 failed")
    }
  }

  it should "exit Success on an empty stream" in {
    run(Nil).asserting {
      case (code, logged) =>
        val _ = code shouldBe ExitCode.Success
        logged.headOption.map(_._2) shouldBe Some("Pipeline completed: 0 processed, 0 succeeded, 0 failed")
    }
  }

  it should "exit Error and log a failure-reasons line when failures occur" in {
    run(
      List(
        ProcessingResult.Succeeded("a"),
        ProcessingResult.Failed("b", "timeout"),
        ProcessingResult.Failed("c", "timeout"),
        ProcessingResult.Failed("d", "bad xml"),
      )
    ).asserting {
      case (code, logged) =>
        val _ = code shouldBe ExitCode.Error
        logged shouldBe Vector(
          ("info", "Pipeline completed: 4 processed, 1 succeeded, 3 failed"),
          ("warn", "Failure reasons: timeout x2; bad xml x1"),
        )
    }
  }

  it should "log and re-raise when the stream itself fails" in {
    val boom = new RuntimeException("stream blew up")
    (for {
      messages <- Ref[IO].of(Vector.empty[(String, String)])
      result <- PipelineExecutor
        .execute[IO](
          Stream.raiseError[IO](boom),
          capturingLogger(messages),
          "test-pipeline",
          runId = 7L,
        )
        .attempt
      logged <- messages.get
    } yield (result, logged)).asserting {
      case (result, logged) =>
        val _ = result shouldBe Left(boom)
        logged shouldBe Vector(("error", "Stream failed: stream blew up"))
    }
  }

  it should "record started + completed on the updater for a clean run" in {
    (for {
      calls  <- Ref[IO].of(Vector.empty[String])
      result <- run(List(ProcessingResult.Succeeded("a")), Some(capturingUpdater(calls)))
      seen   <- calls.get
    } yield (result._1, seen)).asserting {
      case (code, seen) =>
        val _ = code shouldBe ExitCode.Success
        seen shouldBe Vector("started:7:test-pipeline", "completed:7:test-pipeline")
    }
  }

  it should "record started + failed (with counts) on the updater when items fail" in {
    (for {
      calls  <- Ref[IO].of(Vector.empty[String])
      result <- run(List(ProcessingResult.Failed("a", "nope")), Some(capturingUpdater(calls)))
      seen   <- calls.get
    } yield (result._1, seen)).asserting {
      case (code, seen) =>
        val _ = code shouldBe ExitCode.Error
        seen shouldBe Vector("started:7:test-pipeline", "failed:7:test-pipeline:1 of 1 items failed")
    }
  }

  "buildSummary" should "map counts onto workflow statuses" in {
    val now = java.time.Instant.parse("2026-06-10T00:00:00Z")
    def summary(processed: Int, failed: Int) =
      PipelineExecutor.buildSummary(
        StreamingStats(processed, processed - failed, failed, Map.empty),
        stepRunId = 42L,
        pipelineName = "p",
        startedAt = now,
        completedAt = now,
      )
    val _ = summary(0, 0).status shouldBe WorkflowStepStatus.Completed
    val _ = summary(3, 0).status shouldBe WorkflowStepStatus.Completed
    val _ = summary(3, 1).status shouldBe WorkflowStepStatus.CompletedWithErrors
    val _ = summary(3, 3).status shouldBe WorkflowStepStatus.Failed
    summary(3, 1).stepRunId shouldBe 42L
  }

  "formatErrorCounts" should "sort by count desc then reason" in {
    PipelineExecutor.formatErrorCounts(Map("b" -> 1, "a" -> 1, "c" -> 5)) shouldBe "c x5; a x1; b x1"
  }

}
