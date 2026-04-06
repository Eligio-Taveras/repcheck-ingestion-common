package repcheck.ingestion.common.api

import scala.concurrent.duration._

import pureconfig.ConfigReader

final case class HttpClientConfig(
  connectTimeout: FiniteDuration = 10.seconds,
  requestTimeout: FiniteDuration = 30.seconds,
  maxTotalConnections: Int = 10,
  idleTimeout: FiniteDuration = 60.seconds,
) derives ConfigReader
