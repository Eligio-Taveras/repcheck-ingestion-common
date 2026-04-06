package repcheck.ingestion.common.logging

import pureconfig.ConfigReader
import pureconfig.generic.derivation.default.*

final case class LoggingConfig(
  level: String = "INFO",
  jsonOutput: Boolean = true,
) derives ConfigReader
