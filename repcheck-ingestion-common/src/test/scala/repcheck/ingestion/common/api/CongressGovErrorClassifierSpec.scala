package repcheck.ingestion.common.api

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.ErrorClass

class CongressGovErrorClassifierSpec extends AnyFlatSpec with Matchers {

  "CongressGovErrorClassifier" should "classify HTTP 429 as Transient" in {
    val error = CongressGovApiException(429, "Too Many Requests")
    CongressGovErrorClassifier.classify(error) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 500 as Transient" in {
    val error = CongressGovApiException(500, "Internal Server Error")
    CongressGovErrorClassifier.classify(error) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 502 as Transient" in {
    val error = CongressGovApiException(502, "Bad Gateway")
    CongressGovErrorClassifier.classify(error) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 503 as Transient" in {
    val error = CongressGovApiException(503, "Service Unavailable")
    CongressGovErrorClassifier.classify(error) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 504 as Transient" in {
    val error = CongressGovApiException(504, "Gateway Timeout")
    CongressGovErrorClassifier.classify(error) shouldBe ErrorClass.Transient
  }

  it should "classify HTTP 401 as Systemic" in {
    val error = CongressGovApiException(401, "Unauthorized")
    CongressGovErrorClassifier.classify(error) shouldBe ErrorClass.Systemic
  }

  it should "classify HTTP 403 as Systemic" in {
    val error = CongressGovApiException(403, "Forbidden")
    CongressGovErrorClassifier.classify(error) shouldBe ErrorClass.Systemic
  }

  it should "classify HTTP 404 as Systemic" in {
    val error = CongressGovApiException(404, "Not Found")
    CongressGovErrorClassifier.classify(error) shouldBe ErrorClass.Systemic
  }

  it should "classify unknown exceptions as Systemic" in {
    val error = new RuntimeException("Unknown error")
    CongressGovErrorClassifier.classify(error) shouldBe ErrorClass.Systemic
  }

}
