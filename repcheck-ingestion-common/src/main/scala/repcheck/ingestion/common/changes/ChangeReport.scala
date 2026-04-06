package repcheck.ingestion.common.changes

enum ChangeReport[T] {
  case New(entity: T)
  case Updated(diffs: List[FieldDiff])
  case Unchanged()
}
