package repcheck.ingestion.common.execution

import pureconfig.ConfigReader

/**
 * Retry policy for [[DefaultPipelineFailureHandler]] and the initial `max_retries` value written to a
 * `workflow_run_steps` row when a step is first started.
 *
 * `maxRetries` is the maximum number of retries (not total attempts). After `maxRetries` failed retries the step is
 * marked permanently failed. Per Component 2 §2.3 the production default is 3, but the value is intentionally
 * config-driven so different pipelines can pick their own retry budget.
 */
final case class PipelineFailureHandlerConfig(
  maxRetries: Int
) derives ConfigReader
