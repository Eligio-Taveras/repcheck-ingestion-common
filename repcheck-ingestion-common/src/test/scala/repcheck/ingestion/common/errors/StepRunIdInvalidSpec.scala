package repcheck.ingestion.common.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class StepRunIdInvalidSpec extends AnyFlatSpec with Matchers {

  "StepRunIdInvalid" should "include the detail in the message" in {
    val err = StepRunIdInvalid("blank argument")
    val _   = err.getMessage should include("Step run ID")
    err.getMessage should include("blank argument")
  }

}
