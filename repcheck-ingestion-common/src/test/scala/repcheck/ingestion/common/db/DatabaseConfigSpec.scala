package repcheck.ingestion.common.db

import pureconfig.ConfigSource

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DatabaseConfigSpec extends AnyFlatSpec with Matchers {

  "DatabaseConfig" should "load from HOCON with all fields" in {
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
    config.host shouldBe "db.example.com"
    config.port shouldBe 5433
    config.database shouldBe "repcheck"
    config.username shouldBe "admin"
    config.password shouldBe "secret"
    config.maxConnections shouldBe 20
  }

  it should "apply defaults for port and maxConnections" in {
    val hocon =
      """
        |host = "localhost"
        |database = "testdb"
        |username = "user"
        |password = "pass"
        |""".stripMargin

    val config = ConfigSource.string(hocon).loadOrThrow[DatabaseConfig]
    config.port shouldBe 5432
    config.maxConnections shouldBe 10
  }

  it should "compute JDBC URL correctly" in {
    val config = DatabaseConfig(
      host = "db.example.com",
      port = 5433,
      database = "repcheck",
      username = "admin",
      password = "secret",
    )

    config.jdbcUrl shouldBe "jdbc:postgresql://db.example.com:5433/repcheck"
  }

  it should "compute JDBC URL with default port" in {
    val config = DatabaseConfig(
      host = "localhost",
      database = "testdb",
      username = "user",
      password = "pass",
    )

    config.jdbcUrl shouldBe "jdbc:postgresql://localhost:5432/testdb"
  }

}
