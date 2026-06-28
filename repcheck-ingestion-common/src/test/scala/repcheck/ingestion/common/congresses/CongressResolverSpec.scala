package repcheck.ingestion.common.congresses

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all._

import doobie._
import doobie.implicits._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}

/**
 * Direct coverage for [[CongressResolver.resolve]] across all three resolution layers (env var, config field,
 * DB-derived). Uses an injectable `envGetter` so the env-path is exercised without mutating the JVM environment, and an
 * in-memory H2 database so the DB-derived path can be exercised without DockerPostgres — mirroring the per-pipeline
 * resolveCongresses specs this extraction replaces.
 */
class CongressResolverSpec extends AnyFlatSpec with Matchers {

  private val envVar   = "TEST_CONGRESSES"
  private val stepName = "test-pipeline:resolve-congresses"

  /** Stub logger that no-ops every call. The resolver only uses `info`. Avoids null per the no-null-in-tests rule. */
  private class StubLogger extends PipelineLogger[IO] {
    override def info(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def warn(context: LogContext, message: String): IO[Unit]                            = IO.unit
    override def error(context: LogContext, message: String, cause: Option[Throwable]): IO[Unit] = IO.unit
    override def debug(context: LogContext, message: String): IO[Unit]                           = IO.unit
  }

  // Each test gets its own H2 instance so they don't see each other's seeded rows.
  private def freshXa(name: String): Transactor[IO] =
    Transactor.fromDriverManager[IO](
      driver = "org.h2.Driver",
      url = s"jdbc:h2:mem:congressResolver_$name;DB_CLOSE_DELAY=-1",
      user = "",
      password = "",
      logHandler = None,
    )

  private def createBillsTable(xa: Transactor[IO]): IO[Unit] =
    sql"""
      CREATE TABLE bills (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        congress INT
      )
    """.update.run.transact(xa).void

  private def insertBills(xa: Transactor[IO], congresses: List[Int]): IO[Unit] = {
    val inserts = congresses
      .map(c => sql"INSERT INTO bills (congress) VALUES ($c)".update.run)
      .reduceOption(_ *> _)
      .getOrElse(doobie.free.connection.unit)
    inserts.transact(xa).void
  }

  private def resolve(
    xa: Transactor[IO],
    configured: List[Int],
    envGetter: String => Option[String],
  ): List[Int] =
    CongressResolver
      .resolve[IO](envVar, stepName, configured, xa, new StubLogger, envGetter)
      .unsafeRunSync()

  "resolve" should "use the env var when set, ignoring config and DB" in {
    val xa = freshXa("env_path")
    val envGetter: String => Option[String] = {
      case `envVar` => Some("117,118,119")
      case _        => None
    }
    resolve(xa, configured = List(99, 98), envGetter) shouldBe List(117, 118, 119)
  }

  it should "trim whitespace and skip empty tokens in the env var" in {
    val xa = freshXa("env_trim")
    val envGetter: String => Option[String] = {
      case `envVar` => Some("  117 , 118 , , 119  ")
      case _        => None
    }
    resolve(xa, configured = Nil, envGetter) shouldBe List(117, 118, 119)
  }

  it should "treat an empty env var as unset (falls through to next layer)" in {
    val xa = freshXa("env_empty")
    val envGetter: String => Option[String] = {
      case `envVar` => Some("   ")
      case _        => None
    }
    resolve(xa, configured = List(118, 119), envGetter) shouldBe List(118, 119)
  }

  it should "use configuredCongresses when env is unset and config is non-empty" in {
    val xa = freshXa("config_path")
    resolve(xa, configured = List(118, 119), _ => None) shouldBe List(118, 119)
  }

  it should "fall back to the bills table when both env and config are empty" in {
    val xa = freshXa("db_path")
    (createBillsTable(xa) *> insertBills(xa, List(118, 117, 118, 119, 116))).unsafeRunSync()
    // DISTINCT + ORDER BY congress DESC yields each congress once, newest first.
    resolve(xa, configured = Nil, _ => None) shouldBe List(119, 118, 117, 116)
  }

  it should "return an empty list when the DB-derived path finds no bills" in {
    val xa = freshXa("db_empty")
    createBillsTable(xa).unsafeRunSync()
    resolve(xa, configured = Nil, _ => None) shouldBe empty
  }

  it should "skip congress=NULL rows in the DB-derived path" in {
    val xa = freshXa("db_nulls")
    val seed =
      sql"""
        CREATE TABLE bills (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          congress INT
        )
      """.update.run *>
        sql"INSERT INTO bills (congress) VALUES (118)".update.run *>
        sql"INSERT INTO bills (congress) VALUES (NULL)".update.run *>
        sql"INSERT INTO bills (congress) VALUES (119)".update.run
    val _ = seed.transact(xa).unsafeRunSync()
    resolve(xa, configured = Nil, _ => None) shouldBe List(119, 118)
  }

  it should "default envGetter to sys.env.get (env var absent → falls through to config)" in {
    // Exercises the default-arg path. TEST_CONGRESSES is not set in the JVM env, so it falls through to config.
    val xa = freshXa("default_env")
    CongressResolver
      .resolve[IO](envVar, stepName, List(120), xa, new StubLogger)
      .unsafeRunSync() shouldBe List(120)
  }

}
