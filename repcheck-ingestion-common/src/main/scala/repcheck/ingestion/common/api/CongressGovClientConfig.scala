package repcheck.ingestion.common.api

import scala.concurrent.duration._

import pureconfig.ConfigReader
import pureconfig.generic.derivation.default._
import repcheck.pipeline.models.errors.RetryConfig

final case class CongressGovClientConfig(
  apiKey: String,
  baseUrl: String = "https://api.congress.gov/v3",
  pageSize: Int = 250,
  pageDelay: FiniteDuration = Duration.Zero,
  retry: RetryConfig = RetryConfig(),
  http: HttpClientConfig = HttpClientConfig(),
) derives ConfigReader
