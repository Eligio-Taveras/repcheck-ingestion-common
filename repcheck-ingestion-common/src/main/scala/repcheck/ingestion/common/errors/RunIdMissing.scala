package repcheck.ingestion.common.errors

final case class RunIdMissing(detail: String) extends Exception(s"Run ID missing from pipeline arguments: $detail")
