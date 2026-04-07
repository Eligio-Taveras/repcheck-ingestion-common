package repcheck.ingestion.common.errors

final case class ConfigLoadFailed(detail: String, cause: Option[Throwable] = None)
    extends Exception(s"Failed to load pipeline config: $detail") {
  cause.foreach(initCause)
}
