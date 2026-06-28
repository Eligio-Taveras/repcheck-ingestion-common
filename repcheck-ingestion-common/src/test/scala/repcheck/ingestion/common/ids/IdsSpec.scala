package repcheck.ingestion.common.ids

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IdsSpec extends AnyFlatSpec with Matchers {

  "RunId" should "round-trip apply/value" in {
    RunId(123L).value shouldBe 123L
  }

  it should "preserve identity across construction" in {
    RunId(7L) shouldBe RunId(7L)
  }

  "StepRunId" should "round-trip apply/value" in {
    StepRunId(456L).value shouldBe 456L
  }

  it should "preserve identity across construction" in {
    StepRunId(0L) shouldBe StepRunId(0L)
  }

}
