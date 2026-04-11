package repcheck.ingestion.common.events

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventPublishFailedSpec extends AnyFlatSpec with Matchers {

  "EventPublishFailed" should "include topic in message" in {
    val ex = EventPublishFailed("projects/p/topics/t", "connection refused")
    ex.getMessage should include("projects/p/topics/t")
  }

  it should "include detail in message" in {
    val ex = EventPublishFailed("projects/p/topics/t", "connection refused")
    ex.getMessage should include("connection refused")
  }

  it should "set cause when provided" in {
    val cause = new RuntimeException("underlying")
    val ex    = EventPublishFailed("projects/p/topics/t", "failed", Some(cause))
    ex.getCause shouldBe cause
  }

  it should "have no cause when None" in {
    val ex = EventPublishFailed("projects/p/topics/t", "failed", None)
    Option(ex.getCause) shouldBe None
  }

}
