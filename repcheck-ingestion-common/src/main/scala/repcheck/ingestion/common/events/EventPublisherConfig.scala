package repcheck.ingestion.common.events

import pureconfig.ConfigReader

final case class EventPublisherConfig(
  projectId: String,
  topicName: String,
  source: String,
) derives ConfigReader
