package repcheck.ingestion.common.api

final case class PagedResponse[T](
  items: List[T],
  totalCount: Int,
  nextOffset: Option[Int],
)
