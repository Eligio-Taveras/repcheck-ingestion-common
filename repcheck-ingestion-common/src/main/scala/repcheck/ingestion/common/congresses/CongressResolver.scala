package repcheck.ingestion.common.congresses

import cats.effect.{Async, Sync}
import cats.syntax.all._

import doobie.implicits._
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor

import repcheck.ingestion.common.logging.{LogContext, PipelineLogger}
import repcheck.pipeline.models.constants.Tables

/**
 * Shared three-layer congress resolution, extracted verbatim from the per-pipeline `resolveCongresses` copies in
 * votes-pipeline, bill-summary-pipeline, and member-profile-pipeline. The three differed only in their env-var name and
 * a log-step label — both are now parameters.
 *
 * Precedence (highest first):
 *   1. `envVarName` env var (comma-separated, e.g. `"117,118,119"`) — read via `envGetter` because HOCON cannot parse a
 *      string into `List[Int]`. Entries are trimmed; blank tokens dropped; non-numeric tokens raise.
 *   1. `configuredCongresses` (`config.pipeline.congresses`) — forces a specific multi-congress list without env vars.
 *   1. `SELECT DISTINCT congress FROM bills` from the live DB — the pipeline naturally follows whatever congresses the
 *      bills pipeline has covered.
 */
object CongressResolver {

  // SQL-injection safe: Tables.Bills is a compile-time `val` constant from a trusted module — never user input. Spliced
  // unquoted to match the original per-pipeline `resolveCongresses` SQL (and so the identifier resolves the same way on
  // Postgres and H2).
  private val billsTable: Fragment = Fragment.const(Tables.Bills)

  def resolve[F[_]: Async](
    envVarName: String,
    stepName: String,
    configuredCongresses: List[Int],
    xa: Transactor[F],
    logger: PipelineLogger[F],
    envGetter: String => Option[String] = sys.env.get,
  ): F[List[Int]] = {
    val ctx = LogContext("startup", stepName)

    Sync[F].delay(envGetter(envVarName).map(_.trim).filter(_.nonEmpty)).flatMap {
      case Some(raw) =>
        Sync[F]
          .delay(raw.split(",").iterator.map(_.trim).filter(_.nonEmpty).map(_.toInt).toList)
          .flatMap(parsed =>
            logger
              .info(ctx, s"Using ${parsed.size} congresses from $envVarName env: ${parsed.mkString(",")}")
              .as(parsed)
          )
      case None if configuredCongresses.nonEmpty =>
        logger
          .info(
            ctx,
            s"Using ${configuredCongresses.size} congresses from config.pipeline.congresses: " +
              s"${configuredCongresses.mkString(",")}",
          )
          .as(configuredCongresses)
      case None =>
        val query =
          (fr"SELECT DISTINCT congress FROM" ++ billsTable ++
            fr"WHERE congress IS NOT NULL ORDER BY congress DESC")
            .query[Int]
            .to[List]
        for {
          derived <- xa.trans.apply(query)
          _ <- logger.info(
            ctx,
            s"No env or config override — derived ${derived.size} congresses from bills table: ${derived.mkString(",")}",
          )
        } yield derived
    }
  }

}
