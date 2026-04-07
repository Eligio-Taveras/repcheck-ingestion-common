package repcheck.ingestion.common.db

import pureconfig.ConfigReader

/**
 * Database connection settings.
 *
 * All fields are required. Pipeline applications are expected to provide every value via HOCON / environment variables
 * — there are no built-in defaults inside this class. Centralizing connection sizing in app config (rather than burying
 * defaults here) keeps tuning visible to operators.
 */
final case class DatabaseConfig(
  host: String,
  port: Int,
  database: String,
  username: String,
  password: String,
  maxConnections: Int,
) derives ConfigReader {

  def jdbcUrl: String = s"jdbc:postgresql://$host:$port/$database"
}
