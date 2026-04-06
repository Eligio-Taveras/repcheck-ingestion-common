package repcheck.ingestion.common.api

import scala.concurrent.duration.FiniteDuration

import cats.effect.Temporal
import fs2.Stream

trait CongressGovPaginatedClient[F[_], T] {

  protected def pageDelay: FiniteDuration

  implicit protected def temporal: Temporal[F]

  def fetchPage(params: FetchParams): F[PagedResponse[T]]

  def fetchAll(params: FetchParams): Stream[F, T] = {
    val F = temporal
    Stream
      .unfoldEval[F, Option[FetchParams], List[T]](Some(params)) {
        case None => F.pure(None)
        case Some(currentParams) =>
          F.flatMap(fetchPage(currentParams)) { response =>
            val items = response.items
            if (items.size < currentParams.pageSize) {
              F.pure(Some((items, None)))
            } else {
              val nextParams =
                currentParams.copy(offset = currentParams.offset + currentParams.pageSize)
              F.as(F.sleep(pageDelay), Some((items, Some(nextParams))))
            }
          }
      }
      .flatMap(Stream.emits)
  }

}
