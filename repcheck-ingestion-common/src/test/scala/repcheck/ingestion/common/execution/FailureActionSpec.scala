package repcheck.ingestion.common.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FailureActionSpec extends AnyFlatSpec with Matchers {

  "Requeued" should "expose the retry count" in {
    val action: FailureAction = FailureAction.Requeued(2)
    action.retryCount shouldBe 2
  }

  "PermanentlyFailed" should "expose the retry count" in {
    val action: FailureAction = FailureAction.PermanentlyFailed(4)
    action.retryCount shouldBe 4
  }

  "FailureAction" should "match distinct case classes" in {
    val a: FailureAction = FailureAction.Requeued(1)
    val b: FailureAction = FailureAction.PermanentlyFailed(3)
    val _                = a should not equal b
    a shouldBe FailureAction.Requeued(1)
  }

}
