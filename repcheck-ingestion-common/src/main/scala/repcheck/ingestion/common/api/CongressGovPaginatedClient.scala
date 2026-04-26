package repcheck.ingestion.common.api

import scala.concurrent.duration.FiniteDuration

import cats.effect.Temporal

import fs2.Stream

trait CongressGovPaginatedClient[F[_], T] {

  protected def pageDelay: FiniteDuration

  implicit protected def temporal: Temporal[F]

  def fetchPage(params: FetchParams): F[PagedResponse[T]]

  /**
   * Stream every item across all pages for the given query.
   *
   * Termination logic (in priority order):
   *
   *   1. Empty page (`items.isEmpty`) → terminate, end-of-stream (defensive fallback) 2. Reached totalCount (`offset +
   *      pageSize >= totalCount`) → terminate, end-of-stream (authoritative) 3. Anomalous short page mid-stream → retry
   *      up to [[MaxShortPageRetries]] times with backoff. After retries exhaust, accept and continue paginating; the
   *      next page is either full, empty (terminates), or itself short again. 4. Non-empty page → emit items, advance
   *      offset by `pageSize`
   *
   * A page is "anomalously short" when `items.size > 0 && items.size < pageSize` AND the totalCount-bounded end has not
   * yet been reached (`offset + pageSize < totalCount`). Without retries, a single short page mid-stream silently
   * truncates the entire run — observed in production: a global `/v3/bill?fromDateTime=730d` backfill terminated at
   * 7,500 of 342,465 items because of one transient short page (likely the aftermath of a rate-limit recovery, or a
   * server-side timeout returning a partial response).
   *
   * Retry semantics:
   *   - Up to [[MaxShortPageRetries]] retries per anomalous page
   *   - Linear backoff at `pageDelay * 4` between attempts (gives the API ~40 × pageDelay total recovery window)
   *   - Same `(offset, pageSize)` is requested on each retry — we want a full page at THIS offset, not to skip ahead,
   *     because skipping ahead would lose items at the truncated offset that the server WILL return on a clean retry
   *   - After retries exhaust, the latest response is accepted and pagination continues at the next offset; the caller
   *     will eventually see an empty page OR reach totalCount if the API is genuinely done
   */
  private val MaxShortPageRetries: Int = 10

  def fetchAll(params: FetchParams): Stream[F, T] = {
    val F = temporal

    /**
     * Fetch the page at `currentParams`; if the response is anomalously short (positive item count below pageSize AND
     * we haven't reached totalCount), retry with backoff. Returns the eventually-accepted PagedResponse.
     */
    def attemptFetch(currentParams: FetchParams, retriesLeft: Int): F[PagedResponse[T]] =
      F.flatMap(fetchPage(currentParams)) { response =>
        val items         = response.items
        val nextOffset    = currentParams.offset + currentParams.pageSize
        val isLastByCount = response.totalCount > 0 && nextOffset >= response.totalCount
        val isShortPage   = items.nonEmpty && items.size < currentParams.pageSize

        if (isShortPage && !isLastByCount && retriesLeft > 0) {
          // Anomalous short page mid-stream — retry the same offset after a backoff. The longer
          // backoff vs the inter-page delay lets the API's rate-limit token bucket refill or any
          // transient truncation cause clear before we try again.
          F.flatMap(F.sleep(pageDelay * 4L))(_ => attemptFetch(currentParams, retriesLeft - 1))
        } else {
          F.pure(response)
        }
      }

    Stream
      .unfoldEval[F, Option[FetchParams], List[T]](Some(params)) {
        case None => F.pure(None)
        case Some(currentParams) =>
          F.flatMap(attemptFetch(currentParams, MaxShortPageRetries)) { response =>
            val items         = response.items
            val nextOffset    = currentParams.offset + currentParams.pageSize
            val isLastByCount = response.totalCount > 0 && nextOffset >= response.totalCount
            // Terminate when we've reached the end:
            //   - Empty page (defensive fallback for endpoints with missing/zero totalCount)
            //   - Reached/exceeded totalCount (authoritative end-of-stream signal)
            val terminate = items.isEmpty || isLastByCount
            if (terminate) {
              F.pure(Some((items, None)))
            } else {
              val nextParams = currentParams.copy(offset = nextOffset)
              F.as(F.sleep(pageDelay), Some((items, Some(nextParams))))
            }
          }
      }
      .flatMap(Stream.emits)
  }

}
