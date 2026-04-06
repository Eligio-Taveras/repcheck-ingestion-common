package repcheck.ingestion.common.logging

trait PipelineLogger[F[_]] {
  def info(context: LogContext, message: String): F[Unit]
  def warn(context: LogContext, message: String): F[Unit]
  def error(context: LogContext, message: String, cause: Option[Throwable] = None): F[Unit]
  def debug(context: LogContext, message: String): F[Unit]
}
