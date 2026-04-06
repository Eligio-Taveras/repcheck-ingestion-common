package repcheck.ingestion.common.errors

final case class XmlFieldMissing(parentTag: String, fieldTag: String)
    extends Exception(s"Required field '$fieldTag' missing in '$parentTag'")
