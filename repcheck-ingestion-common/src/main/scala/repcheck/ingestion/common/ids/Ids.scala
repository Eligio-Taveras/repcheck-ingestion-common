package repcheck.ingestion.common.ids

/**
 * Type-safe wrappers for the two launcher-supplied identifiers. Both erase to `Long` at runtime (opaque types), so they
 * carry no boxing cost, but the compiler keeps them distinct — a `RunId` can never be passed where a `StepRunId` is
 * expected, and vice-versa. This prevents the args(1)/args(2) mix-up that raw `Long` parameters invite.
 */
object RunId {
  def apply(value: Long): RunId = value

  extension (r: RunId) def value: Long = r
}

opaque type RunId = Long

object StepRunId {
  def apply(value: Long): StepRunId = value

  extension (s: StepRunId) def value: Long = s
}

opaque type StepRunId = Long
