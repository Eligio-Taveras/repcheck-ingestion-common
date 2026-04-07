package repcheck.ingestion.common.db

import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderException

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatabaseConfigSpec extends AnyFlatSpec with Matchers {

  "DatabaseConfig" should "load from HOCON when all fields are provided" in {
    val hocon =
      """
        |host = "db.example.com"
        |port = 5433
        |database = "repcheck"
        |username = "admin"
        |password = "secret"
        |max-connections = 20
        |""".stripMargin

    val config = ConfigSource.string(hocon).loadOrThrow[DatabaseConfig]
    val _      = config.host shouldBe "db.example.com"
    val _      = config.port shouldBe 5433
    val _      = config.database shouldBe "repcheck"
    val _      = config.username shouldBe "admin"
    val _      = config.password shouldBe "secret"
    config.maxConnections shouldBe 20
  }

  it should "fail loading when a required field is missing" in {
    val hocon =
      """
        |host = "localhost"
        |database = "testdb"
        |username = "user"
        |password = "pass"
        |max-connections = 10
        |""".stripMargin

    a[ConfigReaderException[?]] should be thrownBy {
      ConfigSource.string(hocon).loadOrThrow[DatabaseConfig]
    }
  }

  it should "compute JDBC URL from host, port, and database" in {
    val config = DatabaseConfig(
      host = "db.example.com",
      port = 5433,
      database = "repcheck",
      username = "admin",
      password = "secret",
      maxConnections = 5,
    )

    config.jdbcUrl shouldBe "jdbc:postgresql://db.example.com:5433/repcheck"
  }

  it should "use the configured port in the JDBC URL" in {
    val config = DatabaseConfig(
      host = "localhost",
      port = 5432,
      database = "testdb",
      username = "user",
      password = "pass",
      maxConnections = 5,
    )

    config.jdbcUrl shouldBe "jdbc:postgresql://localhost:5432/testdb"
  }

}
