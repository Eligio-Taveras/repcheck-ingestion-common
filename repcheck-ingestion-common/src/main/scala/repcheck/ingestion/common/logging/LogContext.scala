package repcheck.ingestion.common.logging

import java.util.UUID

final case class LogContext(
  runId: String,
  stepName: String,
  correlationId: Option[UUID] = None,
  entityId: Option[String] = None,
  additional: Map[String, String] = Map.empty,
)
