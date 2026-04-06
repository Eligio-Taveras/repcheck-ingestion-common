package repcheck.ingestion.common.db

import pureconfig.ConfigReader

final case class DatabaseConfig(
  host: String,
  port: Int = 5432,
  database: String,
  username: String,
  password: String,
  maxConnections: Int = 10,
) {

  def jdbcUrl: String = s"jdbc:postgresql://$host:$port/$database"
}

object DatabaseConfig {

  given ConfigReader[DatabaseConfig] = ConfigReader.fromCursor { cursor =>
    for {
      obj      <- cursor.asObjectCursor
      host     <- obj.atKey("host").flatMap(_.asString)
      database <- obj.atKey("database").flatMap(_.asString)
      username <- obj.atKey("username").flatMap(_.asString)
      password <- obj.atKey("password").flatMap(_.asString)
      port <- obj.atKeyOrUndefined("port") match {
        case c if c.isUndefined => Right(5432)
        case c                  => c.asInt
      }
      maxConnections <- obj.atKeyOrUndefined("max-connections") match {
        case c if c.isUndefined => Right(10)
        case c                  => c.asInt
      }
    } yield DatabaseConfig(host, port, database, username, password, maxConnections)
  }

}
