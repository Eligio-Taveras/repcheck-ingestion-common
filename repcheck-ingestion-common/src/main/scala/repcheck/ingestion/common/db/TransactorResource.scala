package repcheck.ingestion.common.db

import cats.effect.{Async, Resource}

import doobie.hikari.HikariTransactor
import doobie.util.transactor.Transactor

import com.zaxxer.hikari.HikariConfig

object TransactorResource {

  /**
   * Build a HikariCP-backed `Transactor`.
   *
   * `driverClassName` defaults to PostgreSQL but is a parameter so callers can wire in a different JDBC driver (e.g.,
   * H2 in tests, AlloyDB driver in production overrides) without having to bypass this entry point.
   */
  def make[F[_]: Async](
    config: DatabaseConfig,
    driverClassName: String = "org.postgresql.Driver",
  ): Resource[F, Transactor[F]] =
    makeTransactor[F](
      driverClassName = driverClassName,
      url = config.jdbcUrl,
      user = config.username,
      pass = config.password,
      maxConnections = config.maxConnections,
    )

  private[common] def makeTransactor[F[_]: Async](
    driverClassName: String,
    url: String,
    user: String,
    pass: String,
    maxConnections: Int,
  ): Resource[F, Transactor[F]] = {
    val hikariConfig = new HikariConfig()
    hikariConfig.setDriverClassName(driverClassName)
    hikariConfig.setJdbcUrl(url)
    hikariConfig.setUsername(user)
    hikariConfig.setPassword(pass)
    hikariConfig.setMaximumPoolSize(maxConnections)

    HikariTransactor.fromHikariConfig[F](hikariConfig)
  }

}
