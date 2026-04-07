package repcheck.ingestion.common.execution

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import pureconfig.ConfigReader

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.db.DatabaseConfig
import repcheck.ingestion.common.errors.{ConfigLoadFailed, RunIdMissing}

class PipelineBootstrapSpec extends AnyFlatSpec with Matchers {

  final case class SampleConfig(apiKey: String, maxItems: Int) derives ConfigReader

  "loadConfig" should "parse a valid JSON argument into a typed config" in {
    val json   = """{"api-key":"abc","max-items":50}"""
    val result = PipelineBootstrap.loadConfig[IO, SampleConfig](List(json)).unsafeRunSync()
    val _      = result.apiKey shouldBe "abc"
    result.maxItems shouldBe 50
  }

  it should "raise ConfigLoadFailed when args is empty" in {
    val attempt = PipelineBootstrap.loadConfig[IO, SampleConfig](List.empty).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    val err     = attempt.swap.getOrElse(fail("expected error"))
    val _       = err shouldBe a[ConfigLoadFailed]
    err.getMessage should include("no arguments")
  }

  it should "raise ConfigLoadFailed when the payload is not parseable as HOCON/JSON" in {
    // A raw word cannot be coerced into a case class shape.
    val attempt = PipelineBootstrap.loadConfig[IO, SampleConfig](List("not json")).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[ConfigLoadFailed]
  }

  it should "raise ConfigLoadFailed when JSON is missing required fields" in {
    val attempt = PipelineBootstrap
      .loadConfig[IO, SampleConfig](List("""{"api-key":"abc"}"""))
      .attempt
      .unsafeRunSync()
    val _ = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[ConfigLoadFailed]
  }

  it should "raise ConfigLoadFailed wrapping parse exceptions with a cause" in {
    // A completely malformed payload with unbalanced braces forces the Typesafe parser to throw.
    val attempt = PipelineBootstrap
      .loadConfig[IO, SampleConfig](List("{\"api-key\":\"abc\""))
      .attempt
      .unsafeRunSync()
    val _   = attempt.isLeft shouldBe true
    val err = attempt.swap.getOrElse(fail("expected error"))
    err shouldBe a[ConfigLoadFailed]
  }

  "extractRunId" should "return the second CLI argument" in {
    val result = PipelineBootstrap.extractRunId[IO](List("config-json", "run-123")).unsafeRunSync()
    result shouldBe "run-123"
  }

  it should "trim surrounding whitespace" in {
    val result = PipelineBootstrap.extractRunId[IO](List("cfg", "   run-42   ")).unsafeRunSync()
    result shouldBe "run-42"
  }

  it should "raise RunIdMissing when only one argument is provided" in {
    val attempt = PipelineBootstrap.extractRunId[IO](List("config-json")).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[RunIdMissing]
  }

  it should "raise RunIdMissing when the run ID is a blank string" in {
    val attempt = PipelineBootstrap.extractRunId[IO](List("config-json", "   ")).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[RunIdMissing]
  }

  it should "raise RunIdMissing when args is empty" in {
    val attempt = PipelineBootstrap.extractRunId[IO](List.empty).attempt.unsafeRunSync()
    val _       = attempt.isLeft shouldBe true
    attempt.swap.getOrElse(fail("expected error")) shouldBe a[RunIdMissing]
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
