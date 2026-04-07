package repcheck.ingestion.common.api

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FetchParamsSpec extends AnyFlatSpec with Matchers {

  "FetchParams" should "have sensible defaults" in {
    val params = FetchParams()
    val _      = params.congress shouldBe None
    val _      = params.fromDateTime shouldBe None
    val _      = params.toDateTime shouldBe None
    val _      = params.sort shouldBe SortOrder.UpdateDateDesc
    val _      = params.pageSize shouldBe 250
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
    val _ = params.congress shouldBe Some(118)
    val _ = params.fromDateTime shouldBe Some(from)
    val _ = params.toDateTime shouldBe Some(to)
    val _ = params.sort shouldBe SortOrder.UpdateDateAsc
    val _ = params.pageSize shouldBe 100
    params.offset shouldBe 50
  }

  it should "support copy with modified offset" in {
    val params = FetchParams(pageSize = 100, offset = 0)
    val next   = params.copy(offset = params.offset + params.pageSize)
    val _      = next.offset shouldBe 100
    next.pageSize shouldBe 100
  }

}
