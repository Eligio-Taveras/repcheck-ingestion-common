package repcheck.ingestion.common.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RunIdMissingSpec extends AnyFlatSpec with Matchers {

  "RunIdMissing" should "include the detail in the message" in {
    val err = RunIdMissing("blank argument")
    val _   = err.getMessage should include("Run ID missing")
    err.getMessage should include("blank argument")
  }

}
