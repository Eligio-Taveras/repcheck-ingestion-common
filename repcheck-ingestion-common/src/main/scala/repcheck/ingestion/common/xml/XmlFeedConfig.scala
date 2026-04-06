package repcheck.ingestion.common.xml

import pureconfig.ConfigReader
import pureconfig.generic.derivation.default._
import repcheck.pipeline.models.errors.RetryConfig

final case class HttpClientConfig(
  connectTimeoutMs: Long = 5000L,
  idleTimeoutMs: Long = 60000L,
  requestTimeoutMs: Long = 30000L,
  maxTotalConnections: Int = 10,
) derives ConfigReader

final case class XmlFeedConfig(
  retry: RetryConfig = RetryConfig(
    maxRetries = 3,
    initialBackoffMs = 200L,
    maxBackoffMs = 15000L,
    backoffMultiplier = 2.0,
  ),
  http: HttpClientConfig = HttpClientConfig(),
) derives ConfigReader
