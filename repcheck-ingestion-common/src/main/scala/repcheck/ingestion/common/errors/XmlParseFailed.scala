package repcheck.ingestion.common.errors

final case class XmlParseFailed(url: String, cause: Throwable)
    extends Exception(s"Failed to parse XML from $url", cause)
