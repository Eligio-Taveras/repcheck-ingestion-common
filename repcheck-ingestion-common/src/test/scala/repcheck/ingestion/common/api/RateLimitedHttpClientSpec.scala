package repcheck.ingestion.common.api

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec

import org.http4s.client.Client
import org.http4s.{Request, Response, Uri}

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class RateLimitedHttpClientSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  /** Build a no-op underlying client that increments a counter on each request and returns 200 OK. */
  private def countingClient(counter: java.util.concurrent.atomic.AtomicInteger): Client[IO] =
    Client[IO] { (_: Request[IO]) =>
      Resource.make(IO {
        val _ = counter.incrementAndGet()
        Response[IO]()
      })(_ => IO.unit)
    }

  private val testRequest: Request[IO] =
    Request[IO](uri = Uri.unsafeFromString("https://api.example.com/v3/test"))

  "make" should "wrap an underlying client and forward requests through the rate limiter" in {
    val counter    = new java.util.concurrent.atomic.AtomicInteger(0)
    val underlying = countingClient(counter)

    RateLimitedHttpClient
      .make[IO](underlying, pageDelay = 1.millisecond, permits = 1L)
      .use(wrapped => wrapped.run(testRequest).use(_ => IO.unit))
      .asserting(_ => counter.get() shouldBe 1)
  }

  it should "serialize requests when permits = 1" in {
    // Two concurrent requests with a 50ms pageDelay and permits=1: the second can't even acquire the
    // permit until the first releases (after pageDelay). End-to-end elapsed should be at least
    // pageDelay (50ms) — strict serialization.
    val counter    = new java.util.concurrent.atomic.AtomicInteger(0)
    val underlying = countingClient(counter)

    val program = RateLimitedHttpClient
      .make[IO](underlying, pageDelay = 50.millis, permits = 1L)
      .use { wrapped =>
        for {
          start <- IO.monotonic
          _ <- IO.both(
            wrapped.run(testRequest).use(_ => IO.unit),
            wrapped.run(testRequest).use(_ => IO.unit),
          )
          end <- IO.monotonic
        } yield end - start
      }

    program.asserting { elapsed =>
      val _ = counter.get() shouldBe 2
      elapsed.toMillis should be >= 50L
    }
  }

  it should "allow concurrent requests up to the permit count when permits > 1" in {
    // Two concurrent requests with permits=2 and pageDelay=50ms can BOTH acquire permits immediately.
    // Both run their underlying request concurrently. Total elapsed should still hit the pageDelay
    // because the resource doesn't release until pageDelay has elapsed.
    val counter    = new java.util.concurrent.atomic.AtomicInteger(0)
    val underlying = countingClient(counter)

    val program = RateLimitedHttpClient
      .make[IO](underlying, pageDelay = 50.millis, permits = 2L)
      .use { wrapped =>
        IO.both(
          wrapped.run(testRequest).use(_ => IO.unit),
          wrapped.run(testRequest).use(_ => IO.unit),
        ).void
      }

    program.asserting(_ => counter.get() shouldBe 2)
  }

  it should "release permits after pageDelay so subsequent requests can proceed" in {
    // Sequential requests on permits=1: the second can't fire until the first's pageDelay elapses.
    // We assert at minimum elapsed across two sequential calls.
    val counter    = new java.util.concurrent.atomic.AtomicInteger(0)
    val underlying = countingClient(counter)

    val program = RateLimitedHttpClient
      .make[IO](underlying, pageDelay = 30.millis, permits = 1L)
      .use { wrapped =>
        for {
          start <- IO.monotonic
          _     <- wrapped.run(testRequest).use(_ => IO.unit)
          _     <- wrapped.run(testRequest).use(_ => IO.unit)
          end   <- IO.monotonic
        } yield end - start
      }

    program.asserting { elapsed =>
      val _ = counter.get() shouldBe 2
      elapsed.toMillis should be >= 30L
    }
  }

}
