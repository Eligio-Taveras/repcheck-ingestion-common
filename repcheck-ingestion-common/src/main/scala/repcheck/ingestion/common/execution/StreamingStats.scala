package repcheck.ingestion.common.execution

import repcheck.pipeline.models.metadata.ProcessingResult

/**
 * Stream-friendly run counters, accumulated one [[ProcessingResult]] at a time by [[PipelineExecutor]]'s
 * `compile.fold`. Bounded memory: `errorCounts` keeps only the `(reason -> count)` map — O(distinct-reason-strings),
 * never the full failed payloads. Skipped rolls into succeeded (an idempotent re-delivery is a healthy no-op, not a
 * separate bucket).
 */
final private[execution] case class StreamingStats(
  itemsProcessed: Int,
  itemsSucceeded: Int,
  itemsFailed: Int,
  errorCounts: Map[String, Int],
) {

  def add(result: ProcessingResult): StreamingStats =
    result match {
      case _: ProcessingResult.Succeeded =>
        copy(itemsProcessed = itemsProcessed + 1, itemsSucceeded = itemsSucceeded + 1)
      case _: ProcessingResult.Skipped =>
        copy(itemsProcessed = itemsProcessed + 1, itemsSucceeded = itemsSucceeded + 1)
      case f: ProcessingResult.Failed =>
        val updatedCounts = errorCounts.updatedWith(f.reason) {
          case Some(n) => Some(n + 1)
          case None    => Some(1)
        }
        copy(itemsProcessed = itemsProcessed + 1, itemsFailed = itemsFailed + 1, errorCounts = updatedCounts)
    }

}

private[execution] object StreamingStats {
  val empty: StreamingStats = StreamingStats(0, 0, 0, Map.empty)
}
