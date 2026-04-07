package repcheck.ingestion.common.changes

import difflicious.DiffResult

enum ChangeReport[+T] {
  case New[T](entity: T)                                    extends ChangeReport[T]
  case Updated[T](incoming: T, stored: T, diff: DiffResult) extends ChangeReport[T]
  case Unchanged()                                          extends ChangeReport[Nothing]
}
