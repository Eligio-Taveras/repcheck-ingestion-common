package repcheck.ingestion.common.xml

import pureconfig.ConfigSource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class XmlFeedConfigSpec extends AnyFlatSpec with Matchers {

  "XmlFeedConfig" should "have sensible defaults" in {
    val config = XmlFeedConfig()
    config.retry.maxRetries shouldBe 3
    config.retry.initialBackoffMs shouldBe 200L
    config.retry.maxBackoffMs shouldBe 15000L
    config.retry.backoffMultiplier shouldBe 2.0
    config.http.connectTimeoutMs shouldBe 5000L
    config.http.idleTimeoutMs shouldBe 60000L
    config.http.requestTimeoutMs shouldBe 30000L
    config.http.maxTotalConnections shouldBe 10
  }

  "HttpClientConfig (xml)" should "have sensible defaults" in {
    val config = HttpClientConfig()
    config.connectTimeoutMs shouldBe 5000L
    config.idleTimeoutMs shouldBe 60000L
    config.requestTimeoutMs shouldBe 30000L
    config.maxTotalConnections shouldBe 10
  }

  it should "load from HOCON" in {
    val hocon =
      """
        |retry {
        |  max-retries = 5
        |  initial-backoff-ms = 500
        |  max-backoff-ms = 30000
        |  backoff-multiplier = 3.0
        |}
        |http {
        |  connect-timeout-ms = 10000
        |  idle-timeout-ms = 120000
        |  request-timeout-ms = 60000
        |  max-total-connections = 20
        |}
        |""".stripMargin

    val config = ConfigSource.string(hocon).loadOrThrow[XmlFeedConfig]
    config.retry.maxRetries shouldBe 5
    config.http.maxTotalConnections shouldBe 20
  }

}
