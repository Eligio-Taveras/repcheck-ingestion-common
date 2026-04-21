package repcheck.ingestion.common.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.errors.ErrorClass

class HttpStatusErrorClassifierSpec extends AnyFlatSpec with Matchers {

  // Fixture Throwable carrying a status code via the HttpStatusError trait, for exercising the base class in
  // isolation. Confined to the spec to avoid polluting production's Throwable-subclass uniqueness scan.
  final case class FakeHttpStatusError(statusCode: Int)
      extends RuntimeException(s"Fake HTTP $statusCode")
      with HttpStatusError

  private def makeClassifier(transient: Set[Int]): HttpStatusErrorClassifier[FakeHttpStatusError] =
    new HttpStatusErrorClassifier[FakeHttpStatusError](transient) {}

  "HttpStatusErrorClassifier" should "classify a status code listed as transient as Transient" in {
    val classifier = makeClassifier(Set(429, 500))
    classifier.classify(FakeHttpStatusError(429)) shouldBe ErrorClass.Transient
  }

  it should "classify a status code not listed as Systemic" in {
    val classifier = makeClassifier(Set(429, 500))
    classifier.classify(FakeHttpStatusError(401)) shouldBe ErrorClass.Systemic
  }

  it should "classify a Throwable that does not implement HttpStatusError as Systemic" in {
    val classifier = makeClassifier(Set(429, 500))
    classifier.classify(new RuntimeException("No status")) shouldBe ErrorClass.Systemic
  }

  it should "accept an empty transient set and classify every status as Systemic" in {
    val classifier = makeClassifier(Set.empty)
    val _          = classifier.classify(FakeHttpStatusError(429)) shouldBe ErrorClass.Systemic
    classifier.classify(FakeHttpStatusError(500)) shouldBe ErrorClass.Systemic
  }

  it should "match exactly on the configured transient codes" in {
    val classifier = makeClassifier(Set(418, 429))
    val _          = classifier.classify(FakeHttpStatusError(418)) shouldBe ErrorClass.Transient
    val _          = classifier.classify(FakeHttpStatusError(429)) shouldBe ErrorClass.Transient
    classifier.classify(FakeHttpStatusError(500)) shouldBe ErrorClass.Systemic
  }

}
