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

  // -------------------------------------------------------------------------
  // transientNetworkAware wrapper
  // -------------------------------------------------------------------------

  "HttpStatusErrorClassifier.transientNetworkAware" should
    "classify a wrapped IOException as Transient (network drop / closed socket)" in {
      val base    = makeClassifier(Set(429, 500))
      val wrapped = HttpStatusErrorClassifier.transientNetworkAware(base)
      wrapped.classify(new java.io.IOException("connection reset")) shouldBe ErrorClass.Transient
    }

  it should "classify EmberException as Transient (server closed connection mid-stream)" in {
    val base    = makeClassifier(Set(429, 500))
    val wrapped = HttpStatusErrorClassifier.transientNetworkAware(base)
    val ember   = new org.http4s.ember.core.EmberException.ReachedEndOfStream
    wrapped.classify(ember) shouldBe ErrorClass.Transient
  }

  it should "classify SocketTimeoutException as Transient (subclass of IOException)" in {
    val base    = makeClassifier(Set(429, 500))
    val wrapped = HttpStatusErrorClassifier.transientNetworkAware(base)
    wrapped.classify(new java.net.SocketTimeoutException("read timeout")) shouldBe ErrorClass.Transient
  }

  it should "classify ConnectException as Transient (subclass of IOException)" in {
    val base    = makeClassifier(Set(429, 500))
    val wrapped = HttpStatusErrorClassifier.transientNetworkAware(base)
    wrapped.classify(new java.net.ConnectException("refused")) shouldBe ErrorClass.Transient
  }

  it should "classify java.util.concurrent.TimeoutException as Transient" in {
    val base    = makeClassifier(Set(429, 500))
    val wrapped = HttpStatusErrorClassifier.transientNetworkAware(base)
    wrapped.classify(new java.util.concurrent.TimeoutException("operation timed out")) shouldBe ErrorClass.Transient
  }

  it should "defer a bare RuntimeException to the base classifier" in {
    // Spy classifier that records invocations so we can verify the wrapper called through.
    val seen = new java.util.concurrent.atomic.AtomicReference[List[Throwable]](List.empty)
    val base = new HttpStatusErrorClassifier[FakeHttpStatusError](Set(429, 500)) {
      override def classify(error: Throwable): ErrorClass = {
        val _ = seen.updateAndGet(_ :+ error)
        super.classify(error)
      }
    }
    val wrapped   = HttpStatusErrorClassifier.transientNetworkAware(base)
    val throwable = new RuntimeException("generic")

    val _ = wrapped.classify(throwable) shouldBe ErrorClass.Systemic
    seen.get() shouldBe List(throwable)
  }

  it should "defer an HttpStatusError to the base classifier (boundary case)" in {
    // 500 IS in the transient set on the real classifier; we want to prove the wrapper passed
    // the throwable through to the base rather than short-circuiting via the network walk.
    val base    = makeClassifier(Set(429, 500))
    val wrapped = HttpStatusErrorClassifier.transientNetworkAware(base)
    val _       = wrapped.classify(FakeHttpStatusError(500)) shouldBe ErrorClass.Transient
    val _       = wrapped.classify(FakeHttpStatusError(401)) shouldBe ErrorClass.Systemic
    wrapped.classify(FakeHttpStatusError(404)) shouldBe ErrorClass.Systemic
  }

  it should "walk the cause chain and find a wrapped IOException" in {
    val base      = makeClassifier(Set(429, 500))
    val wrapped   = HttpStatusErrorClassifier.transientNetworkAware(base)
    val rootCause = new java.io.IOException("connection reset by peer")
    val outer     = new RuntimeException("fetch failed", rootCause)
    wrapped.classify(outer) shouldBe ErrorClass.Transient
  }

  it should "walk the cause chain and find a wrapped EmberException" in {
    val base      = makeClassifier(Set(429, 500))
    val wrapped   = HttpStatusErrorClassifier.transientNetworkAware(base)
    val rootCause = new org.http4s.ember.core.EmberException.ReachedEndOfStream
    val outer     = new RuntimeException("fetch failed", rootCause)
    wrapped.classify(outer) shouldBe ErrorClass.Transient
  }

  it should "tolerate a self-referential cause without infinite-looping" in {
    val base    = makeClassifier(Set(429, 500))
    val wrapped = HttpStatusErrorClassifier.transientNetworkAware(base)
    // `Throwable.initCause(this)` throws IllegalArgumentException, so simulate the cycle by
    // overriding getCause() to return self.
    val loop = new RuntimeException("loop") {
      override def getCause: Throwable = this
    }
    wrapped.classify(loop) shouldBe ErrorClass.Systemic
  }

  it should "stop walking after 16 levels of nesting (depth bound) and return Systemic" in {
    val base    = makeClassifier(Set(429, 500))
    val wrapped = HttpStatusErrorClassifier.transientNetworkAware(base)
    // Build a chain of 30 generic RuntimeExceptions terminated by an IOException.
    // The IOException sits beyond the depth bound (16), so the walk should give up before
    // reaching it and the classifier should defer to base, which returns Systemic.
    val deep  = new java.io.IOException("buried network error")
    val chain = (1 to 30).foldLeft[Throwable](deep)((cause, i) => new RuntimeException(s"layer $i", cause))
    wrapped.classify(chain) shouldBe ErrorClass.Systemic
  }

}
