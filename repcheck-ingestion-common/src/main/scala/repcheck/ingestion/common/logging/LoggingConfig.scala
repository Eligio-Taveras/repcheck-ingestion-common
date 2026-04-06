package repcheck.ingestion.common.logging

import pureconfig.ConfigReader

final case class LoggingConfig(
  level: String = "INFO",
  jsonOutput: Boolean = true,
) derives ConfigReader
