package repcheck.ingestion.common.logging

import cats.effect.Sync

import java.time.Instant
import java.time.format.DateTimeFormatter
import io.circe.Json

object PipelineLoggerFactory {

  private val levelOrder: Map[String, Int] = Map(
    "DEBUG" -> 0,
    "INFO"  -> 1,
    "WARN"  -> 2,
    "ERROR" -> 3,
  )

  def make[F[_]: Sync](pipelineName: String): F[PipelineLogger[F]] =
    make[F](pipelineName, LoggingConfig())

  def make[F[_]: Sync](
    pipelineName: String,
    config: LoggingConfig,
  ): F[PipelineLogger[F]] =
    make[F](pipelineName, config, (line: String) => Sync[F].delay(println(line)))

  private[logging] def make[F[_]: Sync](
    pipelineName: String,
    config: LoggingConfig,
    sink: String => F[Unit],
  ): F[PipelineLogger[F]] =
    Sync[F].pure {
      new PipelineLoggerImpl[F](pipelineName, config, sink)
    }

  private class PipelineLoggerImpl[F[_]: Sync](
    pipelineName: String,
    config: LoggingConfig,
    sink: String => F[Unit],
  ) extends PipelineLogger[F] {

    private val configLevelOrd: Int =
      levelOrder.getOrElse(config.level.toUpperCase, 1)

    private def shouldLog(level: String): Boolean = {
      val lvl = levelOrder.getOrElse(level, 0)
      lvl >= configLevelOrd
    }

    override def info(context: LogContext, message: String): F[Unit] =
      log("INFO", context, message, None)

    override def warn(context: LogContext, message: String): F[Unit] =
      log("WARN", context, message, None)

    override def error(
      context: LogContext,
      message: String,
      cause: Option[Throwable] = None,
    ): F[Unit] =
      log("ERROR", context, message, cause)

    override def debug(context: LogContext, message: String): F[Unit] =
      log("DEBUG", context, message, None)

    private def log(
      level: String,
      context: LogContext,
      message: String,
      cause: Option[Throwable],
    ): F[Unit] =
      if (shouldLog(level)) {
        Sync[F].flatMap(Sync[F].delay(Instant.now())) { now =>
          val line = if (config.jsonOutput) {
            formatJson(level, context, message, cause, now)
          } else {
            formatHuman(level, context, message, cause, now)
          }
          sink(line)
        }
      } else {
        Sync[F].unit
      }

    private def formatJson(
      level: String,
      context: LogContext,
      message: String,
      cause: Option[Throwable],
      timestamp: Instant,
    ): String = {
      val baseFields: List[(String, Json)] = List(
        "timestamp" -> Json.fromString(
          DateTimeFormatter.ISO_INSTANT.format(timestamp)
        ),
        "level"    -> Json.fromString(level),
        "message"  -> Json.fromString(message),
        "runId"    -> Json.fromString(context.runId),
        "stepName" -> Json.fromString(context.stepName),
      )

      val correlationField: List[(String, Json)] = context.correlationId.toList.map { id =>
        "correlationId" -> Json.fromString(id.toString)
      }

      val entityField: List[(String, Json)] = context.entityId.toList.map(eid => "entityId" -> Json.fromString(eid))

      val pipelineField: List[(String, Json)] = List(
        "pipeline" -> Json.fromString(pipelineName)
      )

      val additionalFields: List[(String, Json)] =
        context.additional.toList.map {
          case (k, v) =>
            k -> Json.fromString(v)
        }

      val causeField: List[(String, Json)] = cause.toList.map(t => "error" -> Json.fromString(t.getMessage))

      val allFields =
        baseFields ++ correlationField ++ entityField ++ pipelineField ++ additionalFields ++ causeField

      Json.fromFields(allFields).noSpaces
    }

    private def formatHuman(
      level: String,
      context: LogContext,
      message: String,
      cause: Option[Throwable],
      timestamp: Instant,
    ): String = {
      val ts = DateTimeFormatter.ISO_INSTANT.format(timestamp)
      val contextParts = List(
        Some(s"runId=${context.runId}"),
        Some(s"stepName=${context.stepName}"),
        context.correlationId.map(id => s"correlationId=$id"),
        context.entityId.map(eid => s"entityId=$eid"),
        Some(s"pipeline=$pipelineName"),
      ).flatten ++ context.additional.map { case (k, v) => s"$k=$v" }

      val contextStr = contextParts.mkString(", ")
      val causeStr   = cause.fold("")(t => s" | cause: ${t.getMessage}")
      s"$ts [$level] [$pipelineName] [${context.runId}/${context.stepName}] $message {$contextStr}$causeStr"
    }

  }

}
