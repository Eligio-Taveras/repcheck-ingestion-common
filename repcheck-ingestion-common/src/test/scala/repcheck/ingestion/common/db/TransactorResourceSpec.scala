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
