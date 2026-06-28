package repcheck.ingestion.common.execution

import cats.effect.{Async, Resource, Sync}
import cats.syntax.all._

import doobie.util.transactor.Transactor

import pureconfig.{ConfigObjectSource, ConfigReader, ConfigSource}

import repcheck.ingestion.common.db.{DatabaseConfig, TransactorResource}
import repcheck.ingestion.common.errors.{ConfigLoadFailed, RunIdMissing, StepRunIdInvalid}
import repcheck.ingestion.common.ids.{RunId, StepRunId}

/**
 * Standard bootstrap sequence that every pipeline application follows.
 *
 * This is NOT a base class — it is a set of helpers that applications compose inside their own `run` / entry point
 * definitions. Each method is parameterized by `F[_]` at the call site so that callers can pin the effect type (e.g.
 * `PipelineBootstrap.loadConfig[IO, MyConfig](args)`). Putting `F[_]` on the object itself is not possible in Scala.
 */
object PipelineBootstrap {

  /**
   * Layer the first CLI argument as a PureConfig override on top of the application's own defaults.
   *
   * The Launcher serializes each pipeline's per-run configuration overrides as a single JSON string and passes it as
   * `args(0)` (`{}` when there is nothing to override). JSON is a valid subset of HOCON, so PureConfig's string source
   * accepts it as-is. The override is merged with the HIGHEST precedence over `ConfigSource.default` — i.e. the app's
   * `application.conf` (plus any `${?ENV_VAR}` substitutions) supplies the base defaults, and args(0) overrides them.
   * An empty/blank args(0) loads the defaults unchanged.
   *
   * Fails with [[ConfigLoadFailed]] if the payload is not parseable, or the merged config does not match the expected
   * shape.
   */
  def loadConfig[F[_]: Sync, C: ConfigReader](args: List[String]): F[C] = {
    val source: ConfigObjectSource =
      args.lift(0).map(_.trim).filter(_.nonEmpty) match {
        case Some(json) => ConfigSource.string(json).withFallback(ConfigSource.default)
        case None       => ConfigSource.default
      }

    // pureconfig captures every parse/decode error into the Either; Sync.delay additionally lifts any unexpected
    // runtime throw into F's error channel, so an explicit try/catch would be unreachable.
    Sync[F].delay(source.load[C]).flatMap {
      case Right(config) => Sync[F].pure(config)
      case Left(failures) =>
        Sync[F].raiseError[C](ConfigLoadFailed(failures.toList.map(_.description).mkString("; ")))
    }
  }

  /**
   * Read the workflow run ID from the second CLI argument (args(1)) as a `Long` (`workflow_runs.id`).
   *
   * The Launcher assigns a `workflow_runs.id` (BIGINT) for every workflow run and passes it alongside the config JSON.
   * `extractRunId` does not generate a new ID — pipelines must always receive it from the Launcher so that state
   * updates land on the correct `workflow_runs` row.
   *
   * Strict: fails with [[RunIdMissing]] if the run ID is missing, blank, or not a valid `Long`.
   */
  def extractRunId[F[_]: Sync](args: List[String]): F[RunId] = {
    val raw = args.lift(1).map(_.trim).getOrElse("")
    raw.toLongOption match {
      case Some(id) => Sync[F].pure(RunId(id))
      case None     => Sync[F].raiseError[RunId](RunIdMissing(s"run ID (args(1)) missing or non-numeric: '$raw'"))
    }
  }

  /**
   * Read the workflow step run ID from the third CLI argument (args(2)) as a `Long` (`workflow_run_steps.id`).
   *
   * The Launcher assigns a `workflow_run_steps.id` (BIGINT) before invoking the pipeline and passes it through as
   * args(2) alongside the run-level identifier in args(1).
   *
   * Strict: fails with [[StepRunIdInvalid]] if the step run ID is missing, blank, or not a valid `Long`.
   */
  def extractStepRunId[F[_]: Sync](args: List[String]): F[StepRunId] = {
    val raw = args.lift(2).map(_.trim).getOrElse("")
    raw.toLongOption match {
      case Some(id) => Sync[F].pure(StepRunId(id))
      case None =>
        Sync[F].raiseError[StepRunId](StepRunIdInvalid(s"step run ID (args(2)) missing or non-numeric: '$raw'"))
    }
  }

  /**
   * Delegate to [[TransactorResource.make]] to build a HikariCP-backed Doobie transactor. Exposed here so that pipeline
   * applications can wire up their entry points via a single `PipelineBootstrap` import.
   */
  def initTransactor[F[_]: Async](dbConfig: DatabaseConfig): Resource[F, Transactor[F]] =
    TransactorResource.make[F](dbConfig)

}
