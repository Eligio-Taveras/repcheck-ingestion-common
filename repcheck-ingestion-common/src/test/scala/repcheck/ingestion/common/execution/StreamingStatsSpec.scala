package repcheck.ingestion.common.execution

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.pipeline.models.metadata.ProcessingResult

class StreamingStatsSpec extends AnyFlatSpec with Matchers {

  "add" should "count Succeeded as processed + succeeded" in {
    StreamingStats.empty.add(ProcessingResult.Succeeded("a")) shouldBe StreamingStats(1, 1, 0, Map.empty)
  }

  it should "roll Skipped into succeeded" in {
    StreamingStats.empty.add(ProcessingResult.Skipped("a", "dup")) shouldBe StreamingStats(1, 1, 0, Map.empty)
  }

  it should "count Failed and accumulate per-reason counts, never payloads" in {
    val stats = StreamingStats.empty
      .add(ProcessingResult.Failed("a", "timeout"))
      .add(ProcessingResult.Failed("b", "timeout"))
      .add(ProcessingResult.Failed("c", "bad xml"))
    stats shouldBe StreamingStats(3, 0, 3, Map("timeout" -> 2, "bad xml" -> 1))
  }

}
