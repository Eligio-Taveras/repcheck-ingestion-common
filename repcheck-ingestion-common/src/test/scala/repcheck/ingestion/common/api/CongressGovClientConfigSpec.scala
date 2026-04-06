package repcheck.ingestion.common.api

import scala.concurrent.duration._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigSource

class CongressGovClientConfigSpec extends AnyFlatSpec with Matchers {

  "CongressGovClientConfig" should "have sensible Scala defaults" in {
    val config = CongressGovClientConfig(apiKey = "test-key", baseUrl = "https://api.congress.gov/v3")
    config.apiKey shouldBe "test-key"
    config.baseUrl shouldBe "https://api.congress.gov/v3"
    config.pageSize shouldBe 250
    config.pageDelay shouldBe Duration.Zero
    config.retry.maxRetries shouldBe 3
    config.http.connectTimeout shouldBe 10.seconds
  }

  it should "load from HOCON with all fields" in {
    val hocon =
      """
        |api-key = "test-api-key-123"
        |base-url = "https://custom.api.com/v3"
        |page-size = 100
        |page-delay = 200ms
        |retry {
        |  max-retries = 5
        |  initial-backoff-ms = 100
        |  max-backoff-ms = 10000
        |  backoff-multiplier = 3.0
        |}
        |http {
        |  connect-timeout = 5s
        |  request-timeout = 15s
        |  max-total-connections = 20
        |  idle-timeout = 30s
        |}
        |""".stripMargin

    val config = ConfigSource.string(hocon).loadOrThrow[CongressGovClientConfig]

    config.apiKey shouldBe "test-api-key-123"
    config.baseUrl shouldBe "https://custom.api.com/v3"
    config.pageSize shouldBe 100
    config.pageDelay shouldBe 200.millis
    config.retry.maxRetries shouldBe 5
    config.retry.initialBackoffMs shouldBe 100L
    config.retry.maxBackoffMs shouldBe 10000L
    config.retry.backoffMultiplier shouldBe 3.0
    config.http.connectTimeout shouldBe 5.seconds
    config.http.requestTimeout shouldBe 15.seconds
    config.http.maxTotalConnections shouldBe 20
    config.http.idleTimeout shouldBe 30.seconds
  }

  it should "apply defaults when optional fields use defaults" in {
    val hocon =
      """
        |api-key = "minimal-key"
        |base-url = "https://api.congress.gov/v3"
        |page-size = 250
        |page-delay = 0s
        |retry {
        |  max-retries = 3
        |  initial-backoff-ms = 10
        |  max-backoff-ms = 60000
        |  backoff-multiplier = 2.0
        |}
        |http {
        |  connect-timeout = 10s
        |  request-timeout = 30s
        |  max-total-connections = 10
        |  idle-timeout = 60s
        |}
        |""".stripMargin

    val config = ConfigSource.string(hocon).loadOrThrow[CongressGovClientConfig]

    config.apiKey shouldBe "minimal-key"
    config.baseUrl shouldBe "https://api.congress.gov/v3"
    config.pageSize shouldBe 250
    config.pageDelay shouldBe Duration.Zero
    config.retry.maxRetries shouldBe 3
    config.http.connectTimeout shouldBe 10.seconds
    config.http.requestTimeout shouldBe 30.seconds
    config.http.maxTotalConnections shouldBe 10
    config.http.idleTimeout shouldBe 60.seconds
  }

}
