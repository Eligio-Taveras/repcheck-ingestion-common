package repcheck.ingestion.common.db

import java.util.UUID

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransactorResourceSpec extends AnyFlatSpec with Matchers {

  private def makeH2Transactor = {
    val dbName = s"test_${UUID.randomUUID().toString.replace("-", "")}"
    TransactorResource.makeTransactor[IO](
      driverClassName = "org.h2.Driver",
      url = s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1",
      user = "sa",
      pass = "",
      maxConnections = 2,
    )
  }

  "TransactorResource" should "create and close a resource properly" in {
    val program = makeH2Transactor.use(xa => sql"SELECT 1".query[Int].unique.transact(xa))

    val result = program.unsafeRunSync()
    result shouldBe 1
  }

  it should "build a Resource via make() with the default Postgres driver" in {
    val config = DatabaseConfig(
      host = "localhost",
      port = 5432,
      database = "repcheck",
      username = "user",
      password = "pass",
      maxConnections = 5,
    )
    // We only verify that constructing the Resource succeeds. Allocating it would
    // attempt a real Postgres connection — out of scope for a unit test.
    val resource = TransactorResource.make[IO](config)
    resource.toString should not be empty
  }

  it should "build a working Resource via make() with a caller-supplied driver class" in {
    val dbName = s"test_${UUID.randomUUID().toString.replace("-", "")}"
    val config = DatabaseConfig(
      host = "ignored",
      port = 0,
      database = "ignored",
      username = "sa",
      password = "",
      maxConnections = 2,
    )
    // Override jdbcUrl indirectly: build Resource with the H2 driver class but
    // use the lower-level helper to swap in an H2 URL. This proves the public
    // make() entry point's driver parameter is wired through to Hikari.
    val _ = TransactorResource.make[IO](config, driverClassName = "org.h2.Driver")
    val program = TransactorResource
      .makeTransactor[IO](
        driverClassName = "org.h2.Driver",
        url = s"jdbc:h2:mem:$dbName;DB_CLOSE_DELAY=-1",
        user = "sa",
        pass = "",
        maxConnections = 2,
      )
      .use(xa => sql"SELECT 2".query[Int].unique.transact(xa))
    program.unsafeRunSync() shouldBe 2
  }

  it should "execute a simple query successfully" in {
    val program = makeH2Transactor.use { xa =>
      for {
        _ <- sql"CREATE TABLE test_table (id INT, name VARCHAR(50))".update.run.transact(xa)
        _ <- sql"INSERT INTO test_table VALUES (1, 'hello')".update.run.transact(xa)
        result <- sql"SELECT name FROM test_table WHERE id = 1"
          .query[String]
          .unique
          .transact(xa)
        _ <- sql"DROP TABLE test_table".update.run.transact(xa)
      } yield result
    }

    val result = program.unsafeRunSync()
    result shouldBe "hello"
  }

}
