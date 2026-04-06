package repcheck.ingestion.common.events

import pureconfig.ConfigReader

final case class EventPublisherConfig(
  topicName: String,
  source: String,
) derives ConfigReader
