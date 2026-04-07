package repcheck.ingestion.common.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConfigLoadFailedSpec extends AnyFlatSpec with Matchers {

  "ConfigLoadFailed" should "prefix the error message with a standard label" in {
    val err = ConfigLoadFailed("bad payload")
    val _   = err.getMessage should include("Failed to load pipeline config")
    err.getMessage should include("bad payload")
  }

  it should "default the cause to None" in {
    val err = ConfigLoadFailed("detail")
    err.cause shouldBe None
  }

  it should "attach a supplied cause via initCause" in {
    val root = new RuntimeException("boom")
    val err  = ConfigLoadFailed("detail", Some(root))
    val _    = err.cause shouldBe Some(root)
    err.getCause shouldBe root
  }

}
