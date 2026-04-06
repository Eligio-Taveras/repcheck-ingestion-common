package repcheck.ingestion.common.changes

case class FieldDiff(fieldName: String, oldValue: Any, newValue: Any)
