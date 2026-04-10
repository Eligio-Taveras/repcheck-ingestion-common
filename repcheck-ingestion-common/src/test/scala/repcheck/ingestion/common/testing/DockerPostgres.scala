package repcheck.ingestion.common.testing

import java.sql.{Connection, DriverManager}
import java.util.UUID

import scala.annotation.tailrec
import scala.sys.process._
import scala.util.Try

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Resource}

import org.scalatest.{BeforeAndAfterAll, Suite, Tag}
import repcheck.db.migrations.MigrationRunner

/**
 * Immutable AlloyDB Omni container info returned by [[DockerPostgres.resource]].
 */
final case class PostgresContainerInfo(jdbcUrl: String, user: String, password: String) {

  def getConnection: Connection =
    DriverManager.getConnection(jdbcUrl, user, password)

}

/**
 * Cats Effect Resource that manages an AlloyDB Omni Docker container lifecycle for ingestion-common tests.
 *
 * Delegates to `repcheck-db-migrations-runner` for Liquibase-based migration execution, ensuring test schemas match
 * production exactly.
 *
 * Uses the Docker CLI directly rather than Testcontainers to avoid the API version incompatibility between docker-java
 * (v1.32) and Docker 29+ (minimum v1.40).
 *
 * AlloyDB Omni is Google's containerized AlloyDB engine — wire-compatible with PostgreSQL 16, with pgvector and
 * uuid-ossp extensions bundled. Using Omni for tests provides production parity with the AlloyDB instances used in
 * staging and prod.
 *
 * Each resource allocation gets a unique container name and random host port. Migrations are applied automatically
 * after the container is ready.
 *
 * Can be used directly:
 * {{{
 *   DockerPostgres.resource.use { info =>
 *     IO { /* use info.getConnection */ }
 *   }
 * }}}
 *
 * Or via the [[DockerPostgresSpec]] trait for ScalaTest suites.
 */
object DockerPostgres {

  private val dbName: String     = "repcheck_ingestion_test"
  private val dbUser: String     = "test"
  private val dbPassword: String = "test"
  private val image: String      = "google/alloydbomni:16.8.0"
  // AlloyDB Omni runs a post-startup helper that loads dozens of extensions before the database
  // is actually ready for JDBC connections — pg_isready can return success while the helper is
  // still running. The 120s ready window + 60s connect-retry window gives Omni enough room to
  // finish on a slow developer laptop.
  private val maxReadyAttempts: Int = 120
  private val readyDelayMs: Long    = 1000L

  // Internal handle that pairs the container name (for cleanup) with the public info
  final private case class ContainerHandle(name: String, info: PostgresContainerInfo)

  /**
   * Resource that acquires a Docker AlloyDB Omni container with migrations applied, and removes it on release. Each
   * allocation gets a unique container name and random port.
   */
  val resource: Resource[IO, PostgresContainerInfo] =
    Resource.make(acquire)(release).map(_.info)

  private def acquire: IO[ContainerHandle] = IO.blocking {
    val containerName = s"repcheck-ingestion-test-${UUID.randomUUID().toString.take(8)}"
    val port          = startContainer(containerName)
    waitForReady(containerName)
    applyMigrations(port)
    ContainerHandle(
      name = containerName,
      info = PostgresContainerInfo(
        jdbcUrl = s"jdbc:postgresql://localhost:$port/$dbName?sslmode=disable",
        user = dbUser,
        password = dbPassword,
      ),
    )
  }

  private def release(handle: ContainerHandle): IO[Unit] = IO.blocking {
    val _ = Seq("docker", "rm", "-f", handle.name).!
    ()
  }

  private def startContainer(containerName: String): Int = {
    val exitCode = Seq(
      "docker",
      "run",
      "-d",
      "--name",
      containerName,
      "-e",
      s"POSTGRES_DB=$dbName",
      "-e",
      s"POSTGRES_USER=$dbUser",
      "-e",
      s"POSTGRES_PASSWORD=$dbPassword",
      "-p",
      "0:5432",
      image,
    ).!

    if (exitCode != 0) {
      sys.error("Failed to start Docker container. Is Docker running?")
    }

    val portOutput = Seq("docker", "port", containerName, "5432").!!.trim
    // Output format: "0.0.0.0:12345" or "[::]:12345"
    portOutput.split(':').lastOption.getOrElse(sys.error(s"Unexpected docker port output: $portOutput")).toInt
  }

  private def waitForReady(containerName: String): Unit = {
    @tailrec
    def poll(remaining: Int): Boolean =
      if (remaining <= 0) { false }
      else {
        val ready = Try {
          Seq("docker", "exec", containerName, "pg_isready", "-U", dbUser, "-d", dbName).!!
        }.isSuccess

        if (ready) { true }
        else {
          Thread.sleep(readyDelayMs)
          poll(remaining - 1)
        }
      }

    if (!poll(maxReadyAttempts)) {
      val _ = Seq("docker", "rm", "-f", containerName).!
      sys.error(s"PostgreSQL container did not become ready after $maxReadyAttempts attempts")
    }
  }

  private val maxConnectAttempts: Int = 60
  private val connectDelayMs: Long    = 1000L

  private def applyMigrations(port: Int): Unit = {
    val conn = connectWithRetry(port, maxConnectAttempts)
    try
      MigrationRunner.migrate(conn)
    finally conn.close()
  }

  // pg_isready reports success before the server is fully ready for authenticated
  // JDBC connections (especially in CI). Retry the JDBC connect to handle this gap.
  @tailrec
  private def connectWithRetry(port: Int, remaining: Int): Connection = {
    val result = Try {
      DriverManager.getConnection(
        s"jdbc:postgresql://localhost:$port/$dbName?sslmode=disable",
        dbUser,
        dbPassword,
      )
    }
    result match {
      case scala.util.Success(conn) => conn
      case scala.util.Failure(_) if remaining > 1 =>
        Thread.sleep(connectDelayMs)
        connectWithRetry(port, remaining - 1)
      case scala.util.Failure(ex) =>
        sys.error(s"Failed to connect to PostgreSQL after $maxConnectAttempts attempts: ${ex.getMessage}")
    }
  }

}

/**
 * JVM-wide shared AlloyDB Omni container. The first spec to access [[info]] starts a single container and applies
 * migrations; every subsequent spec in the same JVM reuses it. A shutdown hook removes the container when the JVM
 * exits.
 *
 * Sharing is essential for CI efficiency: AlloyDB Omni's image is ~5GB and takes 60–90 seconds to fully start
 * (extensions are loaded by a post-startup helper). Allocating one container per spec would multiply that cost by the
 * number of DB-backed suites; sharing keeps it constant.
 *
 * Test isolation between specs is achieved at the table level — each spec truncates the tables it touches before
 * running, rather than depending on per-suite database isolation.
 */
object SharedDockerPostgres {

  // Resource.allocated returns (A, IO[Unit]) — the value and its finalizer. The lazy val ensures the container is
  // started exactly once per JVM regardless of how many suites mix in DockerPostgresSpec.
  private lazy val handle: (PostgresContainerInfo, IO[Unit]) = {
    val (info, finalizer) = DockerPostgres.resource.allocated.unsafeRunSync()
    val _ = sys.addShutdownHook {
      val _ = finalizer.attempt.unsafeRunSync()
      ()
    }
    (info, finalizer)
  }

  def info: PostgresContainerInfo = handle._1

}

/**
 * Mix-in trait for ScalaTest suites that need an AlloyDB Omni database with the full production schema applied.
 *
 * Backed by [[SharedDockerPostgres]] — all DB-backed specs in the same JVM share a single container. Each spec is
 * responsible for truncating any tables it writes to so its tests do not see leftover state from other suites.
 *
 * Tag your specs with [[DockerRequired]] so CI / local runs can opt in:
 * {{{
 *   class MyRepoSpec extends AnyFlatSpec with DockerPostgresSpec {
 *     it should "query the database" taggedAs DockerRequired in { ... }
 *   }
 * }}}
 */
trait DockerPostgresSpec extends BeforeAndAfterAll { self: Suite =>

  protected def containerInfo: PostgresContainerInfo = SharedDockerPostgres.info

  protected def getConnection: Connection = containerInfo.getConnection
  protected def jdbcUrl: String           = containerInfo.jdbcUrl
  protected def jdbcUser: String          = containerInfo.user
  protected def jdbcPassword: String      = containerInfo.password

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Force lazy val evaluation — starts the shared container if it has not already been started.
    val _ = containerInfo
  }

}

/**
 * Tag for tests that require Docker. Allows selective execution:
 * {{{
 *   sbt "testOnly -- -n DockerRequired"     // only Docker tests
 *   sbt "testOnly -- -l DockerRequired"     // exclude Docker tests
 * }}}
 */
object DockerRequired extends Tag("DockerRequired")
