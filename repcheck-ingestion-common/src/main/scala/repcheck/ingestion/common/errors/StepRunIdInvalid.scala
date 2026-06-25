package repcheck.ingestion.common.errors

/**
 * Raised at pipeline startup when the `stepRunId` CLI argument (args(2)) is missing, blank, or not a valid `Long`. The
 * Launcher assigns a `workflow_run_steps.id` before invoking the pipeline and passes it through alongside the run-level
 * identifier in args(1); a missing or malformed value here means the launcher contract was violated and the run should
 * fail fast.
 */
final case class StepRunIdInvalid(detail: String)
    extends Exception(s"Step run ID missing or invalid in pipeline arguments: $detail")
