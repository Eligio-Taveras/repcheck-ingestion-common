package repcheck.ingestion.common.placeholders

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.transactor.Transactor

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.db.TransactorResource
import repcheck.ingestion.common.testing.{DockerPostgresSpec, DockerRequired}
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.placeholder.HasPlaceholder

class EntityRepositorySpec extends AnyFlatSpec with Matchers with DockerPostgresSpec {

  /**
   * INSERT for the test `members` table. Column order matches the field order of [[MemberDO]] so Doobie's auto-derived
   * `Write[MemberDO]` binds them positionally. The conflict target is `natural_key` (the test schema makes it the
   * primary key) so a duplicate placeholder is silently ignored.
   */
  private val membersInsertSql: String =
    """INSERT INTO members (
      |  member_id, natural_key, first_name, last_name, direct_order_name, inverted_order_name,
      |  honorific_name, birth_year, current_party, state, district, image_url, image_attribution,
      |  official_url, update_date, created_at, updated_at
      |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      |ON CONFLICT (natural_key) DO NOTHING""".stripMargin

  private def withRepo[A](
    block: (Transactor[IO], DoobieEntityRepository[IO, MemberDO]) => IO[A]
  ): A =
    TransactorResource
      .makeTransactor[IO](
        driverClassName = "org.postgresql.Driver",
        url = jdbcUrl,
        user = jdbcUser,
        pass = jdbcPassword,
        maxConnections = 8,
      )
      .use { xa =>
        val repo = new DoobieEntityRepository[IO, MemberDO](xa, membersInsertSql)
        // Each test isolates itself by truncating before running. CASCADE is unnecessary since
        // members has no FKs in the test schema.
        sql"TRUNCATE TABLE members".update.run.transact(xa) >> block(xa, repo)
      }
      .unsafeRunSync()

  private def countByKey(xa: Transactor[IO], key: String): IO[Int] =
    sql"SELECT COUNT(*) FROM members WHERE natural_key = $key".query[Int].unique.transact(xa)

  "DoobieEntityRepository.insertIfNotExists" should "insert a new entity into Postgres" taggedAs DockerRequired in {
    withRepo { (xa, repo) =>
      val placeholder = HasPlaceholder[MemberDO].placeholder("B000444")
      for {
        _     <- repo.insertIfNotExists(placeholder)
        count <- countByKey(xa, "B000444")
      } yield count shouldBe 1
    }
  }

  it should "silently skip on conflict when the natural key already exists" taggedAs DockerRequired in {
    withRepo { (xa, repo) =>
      val placeholder = HasPlaceholder[MemberDO].placeholder("B000444")
      for {
        _     <- repo.insertIfNotExists(placeholder)
        _     <- repo.insertIfNotExists(placeholder)
        count <- countByKey(xa, "B000444")
      } yield count shouldBe 1
    }
  }

  it should "remain consistent under concurrent inserts of the same natural key" taggedAs DockerRequired in {
    // Race-condition guard: 16 fibers all attempt to insert the same placeholder simultaneously.
    // ON CONFLICT DO NOTHING must keep the row count at exactly 1 — no Postgres errors raised
    // and no duplicate rows.
    withRepo { (xa, repo) =>
      val placeholder = HasPlaceholder[MemberDO].placeholder("S000148")
      for {
        _     <- List.fill(16)(repo.insertIfNotExists(placeholder)).parSequence_
        count <- countByKey(xa, "S000148")
      } yield count shouldBe 1
    }
  }

  it should "insert independent rows for distinct natural keys" taggedAs DockerRequired in {
    withRepo { (xa, repo) =>
      val a = HasPlaceholder[MemberDO].placeholder("A000001")
      val b = HasPlaceholder[MemberDO].placeholder("A000002")
      for {
        _      <- repo.insertIfNotExists(a)
        _      <- repo.insertIfNotExists(b)
        countA <- countByKey(xa, "A000001")
        countB <- countByKey(xa, "A000002")
      } yield {
        val _ = countA shouldBe 1
        countB shouldBe 1
      }
    }
  }

}
