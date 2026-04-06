package repcheck.ingestion.common.api

import scala.concurrent.duration._

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Temporal}

import io.circe.{Decoder, Json}

import org.http4s.circe._
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.{Request, Uri}

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class CongressGovPaginatedClientSpec extends AsyncFlatSpec with AsyncIOSpec with Matchers with BeforeAndAfterAll {

  private val wireMock = new WireMockServer(wireMockConfig().dynamicPort())

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMock.start()
  }

  override def afterAll(): Unit = {
    wireMock.stop()
    super.afterAll()
  }

  private def makeClient(
    apiKey: String,
    delay: FiniteDuration = Duration.Zero,
  ): TestPaginatedClient =
    new TestPaginatedClient(
      baseUrl = s"http://localhost:${wireMock.port()}",
      apiKey = apiKey,
      delayBetweenPages = delay,
    )

  /** Test implementation of the paginated client */
  private class TestPaginatedClient(
    baseUrl: String,
    apiKey: String,
    delayBetweenPages: FiniteDuration,
  ) extends CongressGovPaginatedClient[IO, TestItem] {

    override protected def pageDelay: FiniteDuration = delayBetweenPages

    implicit override protected def temporal: Temporal[IO] = IO.asyncForIO

    override def fetchPage(params: FetchParams): IO[PagedResponse[TestItem]] =
      EmberClientBuilder
        .default[IO]
        .build
        .use { client =>
          val baseUri = Uri
            .unsafeFromString(s"$baseUrl/test-endpoint")
            .withQueryParam("api_key", apiKey)
            .withQueryParam("offset", params.offset.toString)
            .withQueryParam("limit", params.pageSize.toString)
            .withQueryParam("sort", params.sort.queryValue)

          val uriWithCongress = params.congress.fold(baseUri)(c => baseUri.withQueryParam("congress", c.toString))

          val uriWithFrom = params.fromDateTime.fold(uriWithCongress) { dt =>
            uriWithCongress.withQueryParam(
              "fromDateTime",
              java.time.format.DateTimeFormatter.ISO_INSTANT.format(dt),
            )
          }

          val uriWithTo = params.toDateTime.fold(uriWithFrom) { dt =>
            uriWithFrom.withQueryParam(
              "toDateTime",
              java.time.format.DateTimeFormatter.ISO_INSTANT.format(dt),
            )
          }

          val request = Request[IO](uri = uriWithTo)

          client.expect[Json](request).flatMap { json =>
            val cursor = json.hcursor
            val items = cursor
              .downField("items")
              .as[List[TestItem]](Decoder.decodeList(TestItem.decoder))
              .getOrElse(List.empty)
            val totalCount =
              cursor.downField("pagination").downField("count").as[Int].getOrElse(0)
            val currentOffset = params.offset
            val nextOffset =
              if (items.size < params.pageSize) { None }
              else { Some(currentOffset + params.pageSize) }

            IO.pure(PagedResponse(items, totalCount, nextOffset))
          }
        }

  }

  final case class TestItem(id: Int, name: String)

  object TestItem {
    implicit val decoder: Decoder[TestItem] = Decoder.forProduct2("id", "name")(TestItem.apply)
  }

  private def stubPage(offset: Int, pageSize: Int, items: List[TestItem], totalCount: Int): Unit = {
    val itemsJson = items
      .map(item => s"""{"id": ${item.id}, "name": "${item.name}"}""")
      .mkString("[", ",", "]")

    val responseBody =
      s"""{
         |  "items": $itemsJson,
         |  "pagination": {
         |    "count": $totalCount,
         |    "next": ${
          if (items.size >= pageSize)
            s""""http://localhost:${wireMock.port()}/test-endpoint?offset=${offset + pageSize}""""
          else "null"
        }
         |  }
         |}""".stripMargin

    val _ = wireMock.stubFor(
      get(urlPathEqualTo("/test-endpoint"))
        .withQueryParam("offset", equalTo(offset.toString))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(responseBody)
        )
    )
  }

  private def makeItems(startId: Int, count: Int): List[TestItem] =
    (startId until (startId + count)).map(i => TestItem(i, s"item-$i")).toList

  "fetchAll" should "paginate until items.size < pageSize collecting all items" in {
    wireMock.resetAll()

    val pageSize   = 250
    val page1Items = makeItems(1, 250)
    val page2Items = makeItems(251, 250)
    val page3Items = makeItems(501, 100)

    stubPage(0, pageSize, page1Items, 600)
    stubPage(250, pageSize, page2Items, 600)
    stubPage(500, pageSize, page3Items, 600)

    val client = makeClient("test-key")
    val params = FetchParams(pageSize = pageSize)

    client.fetchAll(params).compile.toList.asserting { items =>
      items.size shouldBe 600
      items.map(_.id) shouldBe (1 to 600).toList
    }
  }

  it should "inject API key as query parameter on every request" in {
    wireMock.resetAll()

    val pageSize = 250
    val items    = makeItems(1, 100) // less than pageSize = single page
    stubPage(0, pageSize, items, 100)

    val client = makeClient("my-secret-api-key")
    val params = FetchParams(pageSize = pageSize)

    client.fetchAll(params).compile.toList.asserting { result =>
      result.size shouldBe 100

      val requests = wireMock.findAll(getRequestedFor(urlPathEqualTo("/test-endpoint")))
      requests.forEach { req =>
        val _ = req.queryParameter("api_key").firstValue() shouldBe "my-secret-api-key"
      }
      succeed
    }
  }

  it should "respect configured pageDelay between pages" in {
    wireMock.resetAll()

    val pageSize = 250
    stubPage(0, pageSize, makeItems(1, 250), 500)
    stubPage(250, pageSize, makeItems(251, 100), 500)

    val client = makeClient("test-key", delay = 150.millis)
    val params = FetchParams(pageSize = pageSize)

    val timed = for {
      start <- IO.monotonic
      _     <- client.fetchAll(params).compile.toList
      end   <- IO.monotonic
    } yield end - start

    timed.asserting { elapsed =>
      // With 2 pages, there should be at least 1 delay of 150ms
      elapsed.toMillis should be >= 100L
    }
  }

  it should "format fromDateTime and toDateTime as ISO instant in query parameters" in {
    wireMock.resetAll()

    val pageSize = 250
    stubPage(0, pageSize, makeItems(1, 50), 50)

    // Register a catch-all stub that matches any request
    wireMock.stubFor(
      get(urlPathEqualTo("/test-endpoint"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody("""{"items": [], "pagination": {"count": 0}}""")
        )
    )

    val client = makeClient("test-key")
    val from   = java.time.Instant.parse("2024-01-01T00:00:00Z")
    val to     = java.time.Instant.parse("2024-06-30T23:59:59Z")
    val params = FetchParams(
      fromDateTime = Some(from),
      toDateTime = Some(to),
      pageSize = pageSize,
    )

    client.fetchAll(params).compile.toList.asserting { _ =>
      val requests = wireMock.findAll(getRequestedFor(urlPathEqualTo("/test-endpoint")))
      val firstReq = requests.get(0)
      firstReq.queryParameter("fromDateTime").firstValue() shouldBe "2024-01-01T00:00:00Z"
      firstReq.queryParameter("toDateTime").firstValue() shouldBe "2024-06-30T23:59:59Z"
    }
  }

}
