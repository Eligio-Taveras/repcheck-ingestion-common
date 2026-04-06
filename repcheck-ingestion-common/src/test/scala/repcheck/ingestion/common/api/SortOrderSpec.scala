package repcheck.ingestion.common.api

import pureconfig.ConfigSource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SortOrderSpec extends AnyFlatSpec with Matchers {

  "SortOrder" should "have UpdateDateAsc with correct query value" in {
    SortOrder.UpdateDateAsc.queryValue shouldBe "updateDate+asc"
  }

  it should "have UpdateDateDesc with correct query value" in {
    SortOrder.UpdateDateDesc.queryValue shouldBe "updateDate+desc"
  }

  "SortOrder ConfigReader" should "parse updateDate+asc" in {
    val source = ConfigSource.string("""value = "updateDate+asc"""")
    val result = source.at("value").load[SortOrder]
    result shouldBe Right(SortOrder.UpdateDateAsc)
  }

  it should "parse updateDate+desc" in {
    val source = ConfigSource.string("""value = "updateDate+desc"""")
    val result = source.at("value").load[SortOrder]
    result shouldBe Right(SortOrder.UpdateDateDesc)
  }

  it should "default to UpdateDateDesc for unknown values" in {
    val source = ConfigSource.string("value = unknown")
    val result = source.at("value").load[SortOrder]
    result shouldBe Right(SortOrder.UpdateDateDesc)
  }

}
