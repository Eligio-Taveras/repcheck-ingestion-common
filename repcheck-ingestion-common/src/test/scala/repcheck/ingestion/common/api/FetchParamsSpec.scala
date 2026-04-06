package repcheck.ingestion.common.api

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FetchParamsSpec extends AnyFlatSpec with Matchers {

  "FetchParams" should "have sensible defaults" in {
    val params = FetchParams()
    params.congress shouldBe None
    params.fromDateTime shouldBe None
    params.toDateTime shouldBe None
    params.sort shouldBe SortOrder.UpdateDateDesc
    params.pageSize shouldBe 250
    params.offset shouldBe 0
  }

  it should "accept all custom values" in {
    val from = Instant.parse("2024-01-01T00:00:00Z")
    val to   = Instant.parse("2024-12-31T23:59:59Z")
    val params = FetchParams(
      congress = Some(118),
      fromDateTime = Some(from),
      toDateTime = Some(to),
      sort = SortOrder.UpdateDateAsc,
      pageSize = 100,
      offset = 50,
    )
    params.congress shouldBe Some(118)
    params.fromDateTime shouldBe Some(from)
    params.toDateTime shouldBe Some(to)
    params.sort shouldBe SortOrder.UpdateDateAsc
    params.pageSize shouldBe 100
    params.offset shouldBe 50
  }

  it should "support copy with modified offset" in {
    val params = FetchParams(pageSize = 100, offset = 0)
    val next   = params.copy(offset = params.offset + params.pageSize)
    next.offset shouldBe 100
    next.pageSize shouldBe 100
  }

}
