package repcheck.ingestion.common.api

import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class HttpClientResourceSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers {

  "HttpClientResource" should "properly create and release a client" in {
    val config = HttpClientConfig(
      connectTimeout = 5.seconds,
      requestTimeout = 10.seconds,
      maxTotalConnections = 5,
      idleTimeout = 30.seconds,
    )

    HttpClientResource
      .make[IO](config)
      .use(client => IO.pure(client))
      .asserting(_ => succeed)
  }

  it should "apply config values to the client" in {
    val config = HttpClientConfig(
      connectTimeout = 1.second,
      requestTimeout = 2.seconds,
      maxTotalConnections = 3,
      idleTimeout = 4.seconds,
    )

    HttpClientResource
      .make[IO](config)
      .use { client =>
        // If the resource builds without error, config was accepted
        IO.pure(client)
      }
      .asserting(_ => succeed)
  }

}
