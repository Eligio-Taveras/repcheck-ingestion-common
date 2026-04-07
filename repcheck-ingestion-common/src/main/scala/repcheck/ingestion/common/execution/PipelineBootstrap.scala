package repcheck.ingestion.common.execution

import scala.util.control.NonFatal

import cats.effect.{Async, Resource, Sync}
import cats.syntax.all._

import doobie.util.transactor.Transactor

import pureconfig.{ConfigReader, ConfigSource}

import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.errors.{ConfigLoadFailed, RunIdMissing}

/**
 * Standard bootstrap sequence that every pipeline application follows.
 *
 * This is NOT a base class — it is a set of helpers that applications compose inside their own `run` / entry point
 * definitions. Each method is parameterized by `F[_]` at the call site so that callers can pin the effect type (e.g.
 * `PipelineBootstrap.loadConfig[IO, MyConfig](args)`). Putting `F[_]` on the object itself is not possible in Scala.
 */
object PipelineBootstrap {

  /**
   * Parse the first CLI argument as a PureConfig value.
   *
   * The Launcher serializes each pipeline's configuration as a single JSON string and passes it as `args.head`. JSON is
   * a valid subset of HOCON, so PureConfig's string source accepts it as-is. There is no environment variable merging
   * or file fallback — the config for any run is fully captured in `workflow_run_steps.original_message`, which keeps
   * runs reproducible.
   *
   * Fails with [[ConfigLoadFailed]] if `args` is empty, the payload is not parseable, or the parsed config does not
   * match the expected shape.
   */
  def loadConfig[F[_]: Sync, C: ConfigReader](args: List[String]): F[C] =
    args.headOption match {
      case None =>
        Sync[F].raiseError[C](ConfigLoadFailed("no arguments provided; expected config JSON as first argument"))
      case Some(raw) =>
        val attempt: F[Either[ConfigLoadFailed, C]] = Sync[F].delay {
          try
            ConfigSource.string(raw).load[C] match {
              case Right(config) => Right(config)
              case Left(failures) =>
                Left(ConfigLoadFailed(failures.toList.map(_.description).mkString("; ")))
            }
          catch {
            case NonFatal(ex) =>
              Left(ConfigLoadFailed(s"failed to parse config payload: ${ex.getMessage}", Some(ex)))
          }
        }
        attempt.flatMap {
          case Right(config) => Sync[F].pure(config)
          case Left(err)     => Sync[F].raiseError[C](err)
        }
    }

  /**
   * Read the workflow run ID from the second CLI argument.
   *
   * The Launcher generates a UUID for every workflow run and passes it alongside the config JSON. `extractRunId` does
   * not generate a new ID — pipelines must always receive it from the Launcher so that state updates land on the
   * correct `workflow_runs` row.
   *
   * Fails with [[RunIdMissing]] if the run ID is not present or blank.
   */
  def extractRunId[F[_]: Sync](args: List[String]): F[String] =
    args.lift(1) match {
      case Some(id) if id.trim.nonEmpty => Sync[F].pure(id.trim)
      case Some(_)                      => Sync[F].raiseError[String](RunIdMissing("run ID argument was blank"))
      case None => Sync[F].raiseError[String](RunIdMissing("expected run ID as second argument"))
    }

  /**
   * Delegate to [[TransactorResource.make]] to build a HikariCP-backed Doobie transactor. Exposed here so that pipeline
   * applications can wire up their entry points via a single `PipelineBootstrap` import.
   */
  def initTransactor[F[_]: Async](dbConfig: DatabaseConfig): Resource[F, Transactor[F]] =
    TransactorResource.make[F](dbConfig)

}
