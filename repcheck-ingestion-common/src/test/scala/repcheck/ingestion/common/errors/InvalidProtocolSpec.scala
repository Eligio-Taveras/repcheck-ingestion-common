package repcheck.ingestion.common.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class InvalidProtocolSpec extends AnyFlatSpec with Matchers {

  "InvalidProtocol" should "be an Exception with the provided message" in {
    val error = InvalidProtocol("Expected HTTPS but got HTTP")
    error.getMessage shouldBe "Expected HTTPS but got HTTP"
    error shouldBe a[Exception]
  }

  it should "include the message in its string representation" in {
    val error = InvalidProtocol("Bad protocol")
    error.toString should include("Bad protocol")
  }

}
