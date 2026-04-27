package repcheck.ingestion.common.api

import scala.concurrent.duration._

import pureconfig.ConfigReader

import repcheck.pipeline.models.errors.RetryConfig

final case class CongressGovClientConfig(
  apiKey: String,
  baseUrl: String,
  pageSize: Int = 250,
  pageDelay: FiniteDuration = Duration.Zero,
  retry: RetryConfig = RetryConfig(),
  http: HttpClientConfig = HttpClientConfig(),
  shortPageRetries: Int = 10,
) derives ConfigReader
