package repcheck.ingestion.common.logging

import cats.effect.IO
import cats.effect.Ref
import cats.effect.testing.scalatest.AsyncIOSpec
import io.circe.parser.parse
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class PipelineLoggerSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  private val testPipeline = "test-pipeline"

  private def makeTestLogger(
    config: LoggingConfig = LoggingConfig()
  ): IO[(PipelineLogger[IO], Ref[IO, List[String]])] =
    for {
      ref <- Ref.of[IO, List[String]](List.empty)
      sink = (line: String) => ref.update(_ :+ line)
      logger <- PipelineLoggerFactory.make[IO](testPipeline, config, sink)
    } yield (logger, ref)

  private val baseContext = LogContext(
    runId = "run-123",
    stepName = "test-step",
  )

  // --- Every log entry includes runId and stepName ---

  it should "include runId and stepName in every JSON log entry" in {
    for {
      (logger, ref) <- makeTestLogger()
      _             <- logger.info(baseContext, "test message")
      lines         <- ref.get
    } yield {
      lines should have size 1
      val json = parse(lines.headOption.getOrElse("")).fold(e => fail(s"expected valid JSON but got: $e"), identity)
      json.hcursor.get[String]("runId").toOption shouldBe Some("run-123")
      json.hcursor.get[String]("stepName").toOption shouldBe Some("test-step")
    }
  }

  // --- correlationId included when provided ---

  it should "include correlationId when provided in context" in {
    val corrId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
    val ctx    = baseContext.copy(correlationId = Some(corrId))
    for {
      (logger, ref) <- makeTestLogger()
      _             <- logger.info(ctx, "with correlation")
      lines         <- ref.get
    } yield {
      val json = parse(lines.headOption.getOrElse("")).fold(e => fail(s"expected valid JSON but got: $e"), identity)
      json.hcursor.get[String]("correlationId").toOption shouldBe Some(
        "550e8400-e29b-41d4-a716-446655440000"
      )
    }
  }

  it should "omit correlationId when not provided" in {
    for {
      (logger, ref) <- makeTestLogger()
      _             <- logger.info(baseContext, "no correlation")
      lines         <- ref.get
    } yield {
      val json = parse(lines.headOption.getOrElse("")).fold(e => fail(s"expected valid JSON but got: $e"), identity)
      json.hcursor.get[String]("correlationId").toOption shouldBe None
    }
  }

  // --- entityId included when provided ---

  it should "include entityId when provided in context" in {
    val ctx = baseContext.copy(entityId = Some("118-hr-1234"))
    for {
      (logger, ref) <- makeTestLogger()
      _             <- logger.info(ctx, "with entity")
      lines         <- ref.get
    } yield {
      val json = parse(lines.headOption.getOrElse("")).fold(e => fail(s"expected valid JSON but got: $e"), identity)
      json.hcursor.get[String]("entityId").toOption shouldBe Some("118-hr-1234")
    }
  }

  // --- additional map entries appear in output ---

  it should "include additional map entries in JSON output" in {
    val ctx = baseContext.copy(additional = Map("foo" -> "bar", "baz" -> "qux"))
    for {
      (logger, ref) <- makeTestLogger()
      _             <- logger.info(ctx, "with additional")
      lines         <- ref.get
    } yield {
      val json = parse(lines.headOption.getOrElse("")).fold(e => fail(s"expected valid JSON but got: $e"), identity)
      json.hcursor.get[String]("foo").toOption shouldBe Some("bar")
      json.hcursor.get[String]("baz").toOption shouldBe Some("qux")
    }
  }

  // --- Log output is valid JSON when jsonOutput = true ---

  it should "produce valid JSON when jsonOutput is true" in {
    for {
      (logger, ref) <- makeTestLogger(LoggingConfig(jsonOutput = true, level = "DEBUG"))
      _             <- logger.info(baseContext, "json test")
      _             <- logger.warn(baseContext, "warn test")
      _             <- logger.error(baseContext, "error test", Some(new RuntimeException("boom")))
      _             <- logger.debug(baseContext, "debug test")
      lines         <- ref.get
    } yield {
      lines should have size 4
      lines.foreach(line => parse(line).isRight shouldBe true)
    }
  }

  // --- Human-readable output when jsonOutput = false ---

  it should "produce human-readable output when jsonOutput is false" in {
    for {
      (logger, ref) <- makeTestLogger(LoggingConfig(jsonOutput = false))
      _             <- logger.info(baseContext, "human readable")
      lines         <- ref.get
    } yield {
      val line = lines.headOption.getOrElse("")
      line should include("[INFO]")
      line should include(s"[$testPipeline]")
      line should include("[run-123/test-step]")
      line should include("human readable")
      // Should NOT be valid JSON
      parse(line).isLeft shouldBe true
    }
  }

  it should "include additional fields in human-readable output" in {
    val ctx = baseContext.copy(additional = Map("foo" -> "bar"))
    for {
      (logger, ref) <- makeTestLogger(LoggingConfig(jsonOutput = false))
      _             <- logger.info(ctx, "with additional")
      lines         <- ref.get
    } yield {
      val line = lines.headOption.getOrElse("")
      line should include("foo=bar")
    }
  }

  // --- Every log entry includes timestamp and level ---

  it should "include timestamp and level in JSON output" in {
    for {
      (logger, ref) <- makeTestLogger()
      _             <- logger.warn(baseContext, "check fields")
      lines         <- ref.get
    } yield {
      val json = parse(lines.headOption.getOrElse("")).fold(e => fail(s"expected valid JSON but got: $e"), identity)
      json.hcursor.get[String]("timestamp").toOption should not be empty
      json.hcursor.get[String]("level").toOption shouldBe Some("WARN")
    }
  }

  it should "include timestamp in human-readable output" in {
    for {
      (logger, ref) <- makeTestLogger(LoggingConfig(jsonOutput = false))
      _             <- logger.info(baseContext, "ts check")
      lines         <- ref.get
    } yield {
      val line = lines.headOption.getOrElse("")
      // ISO instant format contains T and Z
      line should fullyMatch regex """^\d{4}-\d{2}-\d{2}T.*"""
    }
  }

  // --- Log level filtering respects config ---

  it should "suppress debug and info when config level is WARN" in {
    for {
      (logger, ref) <- makeTestLogger(LoggingConfig(level = "WARN"))
      _             <- logger.debug(baseContext, "should not appear")
      _             <- logger.info(baseContext, "should not appear either")
      _             <- logger.warn(baseContext, "should appear")
      _             <- logger.error(baseContext, "should also appear", None)
      lines         <- ref.get
    } yield {
      lines should have size 2
      val levels = lines.flatMap(line => parse(line).toOption.flatMap(_.hcursor.get[String]("level").toOption))
      levels should contain theSameElementsAs List("WARN", "ERROR")
    }
  }

  it should "suppress debug when config level is INFO" in {
    for {
      (logger, ref) <- makeTestLogger(LoggingConfig(level = "INFO"))
      _             <- logger.debug(baseContext, "should not appear")
      _             <- logger.info(baseContext, "should appear")
      lines         <- ref.get
    } yield lines should have size 1
  }

  it should "allow all levels when config level is DEBUG" in {
    for {
      (logger, ref) <- makeTestLogger(LoggingConfig(level = "DEBUG"))
      _             <- logger.debug(baseContext, "d")
      _             <- logger.info(baseContext, "i")
      _             <- logger.warn(baseContext, "w")
      _             <- logger.error(baseContext, "e", None)
      lines         <- ref.get
    } yield lines should have size 4
  }

  // --- PipelineLoggerFactory.make includes pipeline name in all entries ---

  it should "include pipeline name in all JSON log entries" in {
    for {
      (logger, ref) <- makeTestLogger(LoggingConfig(level = "DEBUG"))
      _             <- logger.info(baseContext, "pipeline check")
      _             <- logger.warn(baseContext, "pipeline check")
      _             <- logger.error(baseContext, "pipeline check", None)
      _             <- logger.debug(baseContext, "pipeline check")
      lines         <- ref.get
    } yield {
      lines should have size 4
      lines.foreach { line =>
        val json = parse(line).fold(e => fail(s"expected valid JSON but got: $e"), identity)
        json.hcursor.get[String]("pipeline").toOption shouldBe Some(testPipeline)
      }
    }
  }

  it should "include pipeline name in human-readable output" in {
    for {
      (logger, ref) <- makeTestLogger(LoggingConfig(jsonOutput = false))
      _             <- logger.info(baseContext, "pipeline check")
      lines         <- ref.get
    } yield {
      val line = lines.headOption.getOrElse("")
      line should include(s"pipeline=$testPipeline")
    }
  }

  // --- Pre-init logging works with fixed context values ---

  it should "support pre-init logging with fixed context values" in {
    val preInitCtx = LogContext(runId = "pre-init", stepName = "bootstrap")
    for {
      (logger, ref) <- makeTestLogger()
      _             <- logger.info(preInitCtx, "Loading config from args")
      lines         <- ref.get
    } yield {
      val json = parse(lines.headOption.getOrElse("")).fold(e => fail(s"expected valid JSON but got: $e"), identity)
      json.hcursor.get[String]("runId").toOption shouldBe Some("pre-init")
      json.hcursor.get[String]("stepName").toOption shouldBe Some("bootstrap")
      json.hcursor.get[String]("message").toOption shouldBe Some("Loading config from args")
    }
  }

  // --- Error logging with cause ---

  it should "include error cause in JSON output when provided" in {
    for {
      (logger, ref) <- makeTestLogger()
      _             <- logger.error(baseContext, "something failed", Some(new RuntimeException("db connection lost")))
      lines         <- ref.get
    } yield {
      val json = parse(lines.headOption.getOrElse("")).fold(e => fail(s"expected valid JSON but got: $e"), identity)
      json.hcursor.get[String]("error").toOption shouldBe Some("db connection lost")
      json.hcursor.get[String]("level").toOption shouldBe Some("ERROR")
    }
  }

  it should "include error cause in human-readable output when provided" in {
    for {
      (logger, ref) <- makeTestLogger(LoggingConfig(jsonOutput = false))
      _             <- logger.error(baseContext, "something failed", Some(new RuntimeException("db down")))
      lines         <- ref.get
    } yield {
      val line = lines.headOption.getOrElse("")
      line should include("cause: db down")
    }
  }

  it should "omit error field when no cause provided" in {
    for {
      (logger, ref) <- makeTestLogger()
      _             <- logger.error(baseContext, "error without cause", None)
      lines         <- ref.get
    } yield {
      val json = parse(lines.headOption.getOrElse("")).fold(e => fail(s"expected valid JSON but got: $e"), identity)
      json.hcursor.get[String]("error").toOption shouldBe None
    }
  }

  // --- Correct log levels for each method ---

  it should "use correct level for each log method" in {
    for {
      (logger, ref) <- makeTestLogger(LoggingConfig(level = "DEBUG"))
      _             <- logger.debug(baseContext, "d")
      _             <- logger.info(baseContext, "i")
      _             <- logger.warn(baseContext, "w")
      _             <- logger.error(baseContext, "e", None)
      lines         <- ref.get
    } yield {
      val levels = lines.flatMap(line => parse(line).toOption.flatMap(_.hcursor.get[String]("level").toOption))
      levels shouldBe List("DEBUG", "INFO", "WARN", "ERROR")
    }
  }

  // --- Full context with all fields ---

  it should "produce complete JSON with all context fields populated" in {
    val corrId = UUID.fromString("550e8400-e29b-41d4-a716-446655440000")
    val ctx = LogContext(
      runId = "bill-ingestion-2024-01-15-a1b2c3d4",
      stepName = "bill-metadata-ingestion",
      correlationId = Some(corrId),
      entityId = Some("118-hr-1234"),
      additional = Map("congress" -> "118"),
    )
    for {
      (logger, ref) <- makeTestLogger()
      _             <- logger.info(ctx, "Processing bill 118-hr-1234")
      lines         <- ref.get
    } yield {
      val json = parse(lines.headOption.getOrElse("")).fold(e => fail(s"expected valid JSON but got: $e"), identity)
      val c    = json.hcursor
      c.get[String]("timestamp").isRight shouldBe true
      c.get[String]("level").toOption shouldBe Some("INFO")
      c.get[String]("message").toOption shouldBe Some("Processing bill 118-hr-1234")
      c.get[String]("runId").toOption shouldBe Some("bill-ingestion-2024-01-15-a1b2c3d4")
      c.get[String]("stepName").toOption shouldBe Some("bill-metadata-ingestion")
      c.get[String]("correlationId").toOption shouldBe Some("550e8400-e29b-41d4-a716-446655440000")
      c.get[String]("entityId").toOption shouldBe Some("118-hr-1234")
      c.get[String]("pipeline").toOption shouldBe Some(testPipeline)
      c.get[String]("congress").toOption shouldBe Some("118")
    }
  }

  // --- Factory method coverage ---

  it should "create a logger via single-arg make and log to stdout" in {
    for {
      logger <- PipelineLoggerFactory.make[IO]("single-arg-pipeline")
      _      <- logger.info(baseContext, "stdout test")
    } yield logger shouldBe a[PipelineLogger[?]]
  }

  it should "create a logger via two-arg make with custom config" in {
    for {
      logger <- PipelineLoggerFactory.make[IO]("two-arg-pipeline", LoggingConfig(level = "WARN", jsonOutput = false))
    } yield logger shouldBe a[PipelineLogger[?]]
  }

  it should "support error without explicit cause parameter" in {
    for {
      (logger, ref) <- makeTestLogger()
      _             <- logger.error(baseContext, "error without cause", None)
      lines         <- ref.get
    } yield {
      val json = parse(lines.headOption.getOrElse("")).fold(e => fail(s"expected valid JSON but got: $e"), identity)
      json.hcursor.get[String]("level").toOption shouldBe Some("ERROR")
      json.hcursor.get[String]("error").toOption shouldBe None
    }
  }

}
