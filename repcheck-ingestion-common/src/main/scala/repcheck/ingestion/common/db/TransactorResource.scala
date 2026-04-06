package repcheck.ingestion.common.db

import cats.effect.{Async, Resource}

import doobie.hikari.HikariTransactor
import doobie.util.transactor.Transactor

import com.zaxxer.hikari.HikariConfig

object TransactorResource {

  private val PostgresDriver: String = "org.postgresql.Driver"

  def make[F[_]: Async](config: DatabaseConfig): Resource[F, Transactor[F]] =
    makeTransactor[F](
      driverClassName = PostgresDriver,
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
