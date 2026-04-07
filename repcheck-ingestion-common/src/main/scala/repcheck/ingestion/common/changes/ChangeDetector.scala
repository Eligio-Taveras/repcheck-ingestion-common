package repcheck.ingestion.common.changes

import java.time.Instant

import difflicious.Differ

object ChangeDetector {

  /**
   * Detects whether `incoming` represents a change relative to `stored`.
   *
   *   - If nothing is stored, the entity is `New`.
   *   - If the stored update timestamp is at or after the incoming one, the entity is `Unchanged` — we never overwrite
   *     with older data.
   *   - Otherwise, a difflicious `Differ[T]` produces a structured `DiffResult`. If the diff is `isOk` (no field-level
   *     differences) the entity is `Unchanged`; otherwise the result is `Updated(incoming, stored, diff)`, with the
   *     diff carried forward so callers can render whatever representation they need (logging, audit, etc.).
   */
  def detect[T](
    incoming: T,
    stored: Option[T],
    incomingUpdateDate: Instant,
    storedUpdateDate: Option[Instant],
  )(using differ: Differ[T]): ChangeReport[T] =
    stored match {
      case None =>
        ChangeReport.New(incoming)
      case Some(existing) =>
        val isStale = storedUpdateDate.exists(s => !incomingUpdateDate.isAfter(s))
        if (isStale) {
          ChangeReport.Unchanged()
        } else {
          val diffResult = differ.diff(existing, incoming)
          if (diffResult.isOk) {
            ChangeReport.Unchanged()
          } else {
            ChangeReport.Updated(incoming, existing, diffResult)
          }
        }
    }

}
