package repcheck.ingestion.common.xml

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.github.tomakehurst.wiremock.stubbing.Scenario
import org.http4s.ember.client.EmberClientBuilder
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import repcheck.ingestion.common.errors.XmlParseFailed
import repcheck.pipeline.models.errors.RetryConfig

class XmlFeedClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val wireMock                    = new WireMockServer(wireMockConfig().dynamicPort())
  implicit private val logger: Logger[IO] = Slf4jLogger.getLogger[IO]

  override def beforeAll(): Unit = {
    super.beforeAll()
    wireMock.start()
  }

  override def afterAll(): Unit = {
    wireMock.stop()
    super.afterAll()
  }

  private val retryConfig: RetryConfig = RetryConfig(
    maxRetries = 2,
    initialBackoffMs = 50L,
    maxBackoffMs = 200L,
    backoffMultiplier = 2.0,
  )

  private def withClient(test: XmlFeedClient[IO] => IO[Unit]): Unit =
    EmberClientBuilder
      .default[IO]
      .build
      .use { httpClient =>
        val xmlClient = XmlFeedClient.make[IO](httpClient, retryConfig)
        test(xmlClient)
      }
      .unsafeRunSync()

  private def baseUrl: String = s"http://localhost:${wireMock.port().toString}"

  "fetchXml" should "fetch and parse valid XML" in {
    wireMock.stubFor(
      get(urlEqualTo("/valid.xml"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml")
            .withBody("<root><name>Test</name></root>")
        )
    )

    withClient { client =>
      client.fetchXml(s"$baseUrl/valid.xml").map { elem =>
        val _ = elem.label shouldBe "root"
        val _ = (elem \ "name").text shouldBe "Test"
      }
    }
  }

  it should "raise XmlParseFailed for malformed content" in {
    wireMock.stubFor(
      get(urlEqualTo("/notxml"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml")
            .withBody("this is not xml at all {{{")
        )
    )

    withClient { client =>
      client.fetchXml(s"$baseUrl/notxml").attempt.map { result =>
        val _ = result.isLeft shouldBe true
        val _ = result.fold(identity, _ => fail("expected Left but got Right")) shouldBe a[XmlParseFailed]
      }
    }
  }

  it should "raise XmlParseFailed for empty response" in {
    wireMock.stubFor(
      get(urlEqualTo("/empty"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml")
            .withBody("")
        )
    )

    withClient { client =>
      client.fetchXml(s"$baseUrl/empty").attempt.map { result =>
        val _ = result.isLeft shouldBe true
        val _ = result.fold(identity, _ => fail("expected Left but got Right")) shouldBe a[XmlParseFailed]
      }
    }
  }

  it should "retry on HTTP 503 and succeed" in {
    val scenarioName = "Retry Scenario"

    wireMock.stubFor(
      get(urlEqualTo("/retry.xml"))
        .inScenario(scenarioName)
        .whenScenarioStateIs(Scenario.STARTED)
        .willReturn(aResponse().withStatus(503))
        .willSetStateTo("first-retry")
    )

    wireMock.stubFor(
      get(urlEqualTo("/retry.xml"))
        .inScenario(scenarioName)
        .whenScenarioStateIs("first-retry")
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml")
            .withBody("<root><status>ok</status></root>")
        )
    )

    withClient { client =>
      client.fetchXml(s"$baseUrl/retry.xml").map { elem =>
        val _ = elem.label shouldBe "root"
        val _ = (elem \ "status").text shouldBe "ok"
      }
    }
  }

  it should "raise XmlParseFailed after all retries exhausted" in {
    wireMock.stubFor(
      get(urlEqualTo("/always-fail.xml"))
        .willReturn(aResponse().withStatus(503))
    )

    withClient { client =>
      client.fetchXml(s"$baseUrl/always-fail.xml").attempt.map { result =>
        val _ = result.isLeft shouldBe true
        val _ = result.fold(identity, _ => fail("expected Left but got Right")) shouldBe a[XmlParseFailed]
      }
    }
  }

  "fetchXmlStream" should "return raw byte stream" in {
    val xmlContent = "<root><data>hello</data></root>"
    wireMock.stubFor(
      get(urlEqualTo("/stream.xml"))
        .willReturn(
          aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/xml")
            .withBody(xmlContent)
        )
    )

    withClient { client =>
      client
        .fetchXmlStream(s"$baseUrl/stream.xml")
        .compile
        .toList
        .map { bytes =>
          val body = new String(bytes.toArray, "UTF-8")
          val _    = body shouldBe xmlContent
        }
    }
  }

}
