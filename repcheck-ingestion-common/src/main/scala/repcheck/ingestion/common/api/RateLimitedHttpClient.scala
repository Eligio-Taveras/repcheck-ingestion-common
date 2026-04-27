package repcheck.ingestion.common.api

import scala.concurrent.duration.FiniteDuration

import cats.effect.std.Semaphore
import cats.effect.{Async, Resource, Temporal}
import cats.syntax.all._

import org.http4s.client.Client

/**
 * Centralized rate limiter for outbound http4s clients. Wraps an underlying `Client[F]` such that:
 *
 *   - At most `permits` requests may be in flight at any time (a `Semaphore` enforces the bound).
 *   - After each request finishes (whether the response body is consumed or not), the helper sleeps for `pageDelay`
 *     before releasing the permit.
 *
 * Callers acquire one permit per `Client.run` invocation, run the underlying request, and the permit is released after
 * the configured delay — so concurrent callers naturally space out across the `pageDelay` window. With `permits = 1`
 * the wrapper enforces strictly serial execution; with `permits > 1` the wrapper caps fan-out at that bound while still
 * pacing each finished request.
 *
 * Used by every Congress.gov-consuming pipeline (bill-metadata, bill-summary, bill-text,
 * bill-text-availability-checker, member-profile, votes) so the per-pipeline call rate stays independent of the others
 * sharing the same API key — and so one canonical implementation lives here instead of being copy-pasted into each
 * pipeline's `app` package.
 *
 * The `pageDelay` parameter mirrors `CongressGovClientConfig.pageDelay`; pipelines pass `config.pageDelay` directly.
 * The `permits` parameter mirrors each pipeline's own concurrency knob (e.g. `BillSummaryConfig.httpConcurrency`).
 */
object RateLimitedHttpClient {

  /**
   * Build a rate-limited wrapper around an underlying `Client[F]`.
   *
   * @param underlying
   *   the unrate-limited HTTP client (typically the Ember client built once at app startup).
   * @param pageDelay
   *   delay enforced AFTER each request completes, before the permit returns to the semaphore.
   * @param permits
   *   maximum concurrent in-flight requests. `1` (typical) gives strictly serial execution.
   */
  def make[F[_]: Async](
    underlying: Client[F],
    pageDelay: FiniteDuration,
    permits: Long,
  ): Resource[F, Client[F]] =
    Resource.eval(Semaphore[F](permits)).map { sem =>
      Client[F] { request =>
        Resource.make(sem.acquire)(_ => Temporal[F].sleep(pageDelay) >> sem.release) >>
          underlying.run(request)
      }
    }

}
