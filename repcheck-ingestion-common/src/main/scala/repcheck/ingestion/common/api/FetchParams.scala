package repcheck.ingestion.common.api

import java.time.Instant

final case class FetchParams(
  congress: Option[Int] = None,
  fromDateTime: Option[Instant] = None,
  toDateTime: Option[Instant] = None,
  sort: SortOrder = SortOrder.UpdateDateDesc,
  pageSize: Int = 250,
  offset: Int = 0,
)
