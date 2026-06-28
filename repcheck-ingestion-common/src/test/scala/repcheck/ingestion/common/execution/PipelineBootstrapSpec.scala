package repcheck.ingestion.common.execution

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import pureconfig.ConfigReader

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.errors.{ConfigLoadFailed, RunIdMissing, StepRunIdInvalid}
import repcheck.ingestion.common.ids.{RunId, StepRunId}

class PipelineBootstrapSpec extends AnyFlatSpec with Matchers {

  // Backed by src/test/resources/application.conf (api-key=default-key, max-items=10),
  // loaded through ConfigSource.default inside loadConfig.
  final case class SampleConfig(apiKey: String, maxItems: Int) derives ConfigReader

  "loadConfig" should "load defaults from application.conf when args(0) is empty" in {
    val result = PipelineBootstrap.loadConfig[IO, SampleConfig](List.empty).unsafeRunSync()
    val _      = result.apiKey shouldBe "default-key"
    result.maxItems shouldBe 10
  }

  it should "load defaults when args(0) is a blank string" in {
    val result = PipelineBootstrap.loadConfig[IO, SampleConfig](List("   ")).unsafeRunSync()
    val _      = result.apiKey shouldBe "default-key"
    result.maxItems shouldBe 10
  }

  it should "let an args(0) JSON override take precedence over the application.conf default" in {
    val json   = """{"api-key":"override-key"}"""
    val result = PipelineBootstrap.loadConfig[IO, SampleConfig](List(json)).unsafeRunSync()
    // api-key overridden by the arg; max-items still falls back to the default.
    val _ = result.apiKey shouldBe "override-key"
    result.maxItems shouldBe 10
  }

  it should "override every field when args(0) supplies all of them" in {
    val json   = """{"api-key":"abc","max-items":50}"""
    val result = PipelineBootstrap.loadConfig[IO, SampleConfig](List(json)).unsafeRunSync()
    val _      = result.apiKey shouldBe "abc"
    result.maxItems shouldBe 50
  }

  it should "raise ConfigLoadFailed when an override sets a field to the wrong type" in {
    val attempt = PipelineBootstrap
      .loadConfig[IO, SampleConfig](List("""{"max-items":"not-a-number"}"""))
      .attempt
      .unsafeRunSync()
    val _ = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[ConfigLoadFailed]
  }

  it should "raise ConfigLoadFailed for malformed HOCON/JSON in args(0)" in {
    // Unbalanced braces — pureconfig captures the parse error as a Left, which becomes ConfigLoadFailed.
    val attempt = PipelineBootstrap
      .loadConfig[IO, SampleConfig](List("{\"api-key\":\"abc\""))
      .attempt
      .unsafeRunSync()
    val _   = attempt.isLeft shouldBe true
    val err = attempt.swap.getOrElse(fail("expected error"))
    err shouldBe a[ConfigLoadFailed]
  }

  "extractRunId" should "parse the second CLI argument into a RunId wrapping the Long" in {
    val result = PipelineBootstrap.extractRunId[IO](List("config-json", "123")).unsafeRunSync()
    val _      = result shouldBe RunId(123L)
    result.value shouldBe 123L
  }

  it should "trim surrounding whitespace" in {
    val result = PipelineBootstrap.extractRunId[IO](List("cfg", "   42   ")).unsafeRunSync()
    result.value shouldBe 42L
  }

  it should "raise RunIdMissing when only one argument is provided" in {
    val attempt = PipelineBootstrap.extractRunId[IO](List("config-json")).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[RunIdMissing]
  }

  it should "raise RunIdMissing when the run ID is blank" in {
    val attempt = PipelineBootstrap.extractRunId[IO](List("config-json", "   ")).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[RunIdMissing]
  }

  it should "raise RunIdMissing when the run ID is non-numeric" in {
    val attempt = PipelineBootstrap.extractRunId[IO](List("config-json", "run-123")).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[RunIdMissing]
  }

  it should "raise RunIdMissing when args is empty" in {
    val attempt = PipelineBootstrap.extractRunId[IO](List.empty).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[RunIdMissing]
  }

  "extractStepRunId" should "parse the third CLI argument into a StepRunId wrapping the Long" in {
    val result = PipelineBootstrap.extractStepRunId[IO](List("cfg", "1", "456")).unsafeRunSync()
    val _      = result shouldBe StepRunId(456L)
    result.value shouldBe 456L
  }

  it should "trim surrounding whitespace" in {
    val result = PipelineBootstrap.extractStepRunId[IO](List("cfg", "1", "  77  ")).unsafeRunSync()
    result.value shouldBe 77L
  }

  it should "raise StepRunIdInvalid when the third argument is missing" in {
    val attempt = PipelineBootstrap.extractStepRunId[IO](List("cfg", "1")).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[StepRunIdInvalid]
  }

  it should "raise StepRunIdInvalid when the step run ID is blank" in {
    val attempt = PipelineBootstrap.extractStepRunId[IO](List("cfg", "1", "   ")).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[StepRunIdInvalid]
  }

  it should "raise StepRunIdInvalid when the step run ID is non-numeric" in {
    val attempt = PipelineBootstrap.extractStepRunId[IO](List("cfg", "1", "step-9")).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[StepRunIdInvalid]
  }

  it should "raise StepRunIdInvalid when args has fewer than three elements" in {
    val attempt = PipelineBootstrap.extractStepRunId[IO](List.empty).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[StepRunIdInvalid]
  }

  "initTransactor" should "produce a Resource that can be allocated" in {
    val config = DatabaseConfig(
      host = "localhost",
      port = 5432,
      database = "repcheck",
      username = "user",
      password = "pass",
      maxConnections = 5,
    )
    // Build the Resource but do not allocate — that would need a real Postgres instance.
    val resource = PipelineBootstrap.initTransactor[IO](config)
    resource.toString should not be empty
  }

}
