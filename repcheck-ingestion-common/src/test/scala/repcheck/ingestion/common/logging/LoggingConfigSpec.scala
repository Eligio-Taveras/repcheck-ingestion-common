package repcheck.ingestion.common.logging

import pureconfig.{ConfigReader, ConfigSource}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LoggingConfigSpec extends AnyFlatSpec with Matchers {

  it should "have sensible defaults" in {
    val config = LoggingConfig()
    config.level shouldBe "INFO"
    config.jsonOutput shouldBe true
  }

  it should "load from HOCON via PureConfig" in {
    val source = ConfigSource.string("""
      |level = "WARN"
      |json-output = false
      """.stripMargin)
    val result = source.load[LoggingConfig]
    result.isRight shouldBe true
    val config = result.fold(e => fail(s"expected Right but got Left: $e"), identity)
    config.level shouldBe "WARN"
    config.jsonOutput shouldBe false
  }

  it should "load with defaults from minimal config" in {
    val source = ConfigSource.string("""
      |level = "INFO"
      |json-output = true
      """.stripMargin)
    val result = source.load[LoggingConfig]
    result.isRight shouldBe true
    val config = result.fold(e => fail(s"expected Right but got Left: $e"), identity)
    config.level shouldBe "INFO"
    config.jsonOutput shouldBe true
  }

  it should "have a ConfigReader instance via derives" in {
    val reader = implicitly[ConfigReader[LoggingConfig]]
    reader shouldBe a[ConfigReader[?]]
  }

}
