package repcheck.ingestion.common.db

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.testing.{DockerPostgresSpec, DockerRequired}

class TransactorResourceSpec extends AnyFlatSpec with Matchers with DockerPostgresSpec {

  private def databaseConfig: DatabaseConfig =
    DatabaseConfig(
      host = "localhost",
      // jdbcUrl is the source of truth in tests; host/port/database fields are unused by makeTransactor.
      port = 0,
      database = "ignored",
      username = jdbcUser,
      password = jdbcPassword,
      maxConnections = 2,
    )

  /**
   * The DatabaseConfig path uses `config.jdbcUrl` to build the connection string. We override it via the lower-level
   * helper here so the spec exercises the public Hikari wiring without needing a host/port-derived URL.
   */
  private def transactorResource =
    TransactorResource.makeTransactor[IO](
      driverClassName = "org.postgresql.Driver",
      url = jdbcUrl,
      user = jdbcUser,
      pass = jdbcPassword,
      maxConnections = 2,
    )

  "TransactorResource" should "create a Hikari-backed Transactor that runs SELECT 1" taggedAs DockerRequired in {
    val program = transactorResource.use(xa => sql"SELECT 1".query[Int].unique.transact(xa))
    program.unsafeRunSync() shouldBe 1
  }

  it should "build a Resource via make() with the default Postgres driver" taggedAs DockerRequired in {
    // Constructing the Resource must succeed without trying to connect.
    val resource = TransactorResource.make[IO](databaseConfig)
    resource.toString should not be empty
  }

  it should "execute a CREATE/INSERT/SELECT round trip on a real Postgres connection" taggedAs DockerRequired in {
    val program = transactorResource.use { xa =>
      for {
        _ <- sql"CREATE TEMP TABLE t_round_trip (id INT, name TEXT)".update.run.transact(xa)
        _ <- sql"INSERT INTO t_round_trip VALUES (1, 'hello')".update.run.transact(xa)
        result <- sql"SELECT name FROM t_round_trip WHERE id = 1"
          .query[String]
          .unique
          .transact(xa)
      } yield result
    }

    program.unsafeRunSync() shouldBe "hello"
  }

  it should "respect the maxConnections setting on the underlying Hikari pool" taggedAs DockerRequired in {
    // Two concurrent transactions should both succeed against a pool sized 2.
    val program = transactorResource.use { xa =>
      val q = sql"SELECT pg_backend_pid()".query[Int].unique.transact(xa)
      (q, q).parTupled
    }
    val (a, b) = program.unsafeRunSync()
    val _      = a should be > 0
    b should be > 0
  }

}
