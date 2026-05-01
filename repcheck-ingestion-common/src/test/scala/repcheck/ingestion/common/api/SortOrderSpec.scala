package repcheck.ingestion.common.api

import pureconfig.ConfigSource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SortOrderSpec extends AnyFlatSpec with Matchers {

  // The queryValue is the DECODED form of the Congress.gov `sort` query param. The API expects
  // `"updateDate asc"` / `"updateDate desc"` — a literal space between field and direction.
  // http4s URL-encodes the space (to %20 or +, both of which the server decodes back to space),
  // so the value here is what the server sees post-decode. Anything other than a space here
  // (e.g., a literal `+`) silently falls back to a non-updateDate order on the API side.
  "SortOrder" should "have UpdateDateAsc with the space-separated query value the API expects" in {
    SortOrder.UpdateDateAsc.queryValue shouldBe "updateDate asc"
  }

  it should "have UpdateDateDesc with the space-separated query value the API expects" in {
    SortOrder.UpdateDateDesc.queryValue shouldBe "updateDate desc"
  }

  "SortOrder ConfigReader" should "parse the canonical 'updateDate asc' form" in {
    val source = ConfigSource.string("""value = "updateDate asc"""")
    val result = source.at("value").load[SortOrder]
    result shouldBe Right(SortOrder.UpdateDateAsc)
  }

  it should "parse the canonical 'updateDate desc' form" in {
    val source = ConfigSource.string("""value = "updateDate desc"""")
    val result = source.at("value").load[SortOrder]
    result shouldBe Right(SortOrder.UpdateDateDesc)
  }

  // Backwards-compat: pre-fix HOCON configs may have `sort = "updateDate+asc"` — the ConfigReader
  // accepts those too so deployments don't break on rollout. The queryValue on the parsed enum is
  // the new (correct) form regardless of which input string the config used.
  it should "still parse the legacy 'updateDate+asc' form (backwards-compat)" in {
    val source = ConfigSource.string("""value = "updateDate+asc"""")
    val result = source.at("value").load[SortOrder]
    result shouldBe Right(SortOrder.UpdateDateAsc)
  }

  it should "still parse the legacy 'updateDate+desc' form (backwards-compat)" in {
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
