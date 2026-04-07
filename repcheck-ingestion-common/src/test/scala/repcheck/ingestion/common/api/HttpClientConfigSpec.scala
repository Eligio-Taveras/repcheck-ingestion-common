package repcheck.ingestion.common.api

import scala.concurrent.duration._

import pureconfig.ConfigSource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HttpClientConfigSpec extends AnyFlatSpec with Matchers {

  "HttpClientConfig" should "have sensible defaults" in {
    val config = HttpClientConfig()
    val _      = config.connectTimeout shouldBe 10.seconds
    val _      = config.requestTimeout shouldBe 30.seconds
    val _      = config.maxTotalConnections shouldBe 10
    config.idleTimeout shouldBe 60.seconds
  }

  it should "load from HOCON" in {
    val hocon =
      """
        |connect-timeout = 5s
        |request-timeout = 15s
        |max-total-connections = 20
        |idle-timeout = 45s
        |""".stripMargin

    val config = ConfigSource.string(hocon).loadOrThrow[HttpClientConfig]
    val _      = config.connectTimeout shouldBe 5.seconds
    val _      = config.requestTimeout shouldBe 15.seconds
    val _      = config.maxTotalConnections shouldBe 20
    config.idleTimeout shouldBe 45.seconds
  }

}
