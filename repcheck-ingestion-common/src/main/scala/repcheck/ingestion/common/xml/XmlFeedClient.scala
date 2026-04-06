package repcheck.ingestion.common.xml

import scala.xml.Elem

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import fs2.Stream
import org.http4s.Request
import org.http4s.Uri
import org.http4s.client.Client
import org.http4s.scalaxml._
import org.typelevel.log4cats.Logger
import repcheck.ingestion.common.errors.XmlParseFailed
import repcheck.pipeline.models.errors.RetryConfig

trait XmlFeedClient[F[_]] {
  def fetchXml(url: String): F[Elem]
  def fetchXmlStream(url: String): Stream[F, Byte]
}

object XmlFeedClient {

  def make[F[_]: Async: Logger](
    client: Client[F],
    retryConfig: RetryConfig,
  ): XmlFeedClient[F] =
    new XmlFeedClientImpl[F](client, retryConfig)

  final private class XmlFeedClientImpl[F[_]: Async: Logger](
    client: Client[F],
    retryConfig: RetryConfig,
  ) extends XmlFeedClient[F] {

    override def fetchXml(url: String): F[Elem] = {
      val request = buildRequest(url)
      request.flatMap { req =>
        retryWithBackoff(retryConfig) {
          client
            .expect[Elem](req)
            .adaptError {
              case e: Throwable =>
                XmlParseFailed(url, e): Throwable
            }
        }
      }
    }

    override def fetchXmlStream(url: String): Stream[F, Byte] =
      Stream.eval(buildRequest(url)).flatMap(req => client.stream(req).flatMap(_.body))

    private def buildRequest(url: String): F[Request[F]] =
      MonadThrow[F].fromEither(
        Uri
          .fromString(url)
          .leftMap(e => XmlParseFailed(url, e): Throwable)
          .map(uri => Request[F](uri = uri))
      )

    private def retryWithBackoff[A](config: RetryConfig)(fa: F[A]): F[A] = {
      def attempt(remaining: Int, backoffMs: Long): F[A] =
        fa.handleErrorWith { error =>
          if (remaining > 0) {
            Logger[F].warn(
              s"Retryable error, ${remaining.toString} retries left, backoff ${backoffMs.toString}ms: ${error.getMessage}"
            ) *>
              Async[F].sleep(
                scala.concurrent.duration.FiniteDuration(
                  backoffMs,
                  scala.concurrent.duration.MILLISECONDS,
                )
              ) *>
              attempt(
                remaining - 1,
                Math.min(
                  (backoffMs.toDouble * config.backoffMultiplier).toLong,
                  config.maxBackoffMs,
                ),
              )
          } else {
            MonadThrow[F].raiseError(error)
          }
        }
      attempt(config.maxRetries, config.initialBackoffMs)
    }

  }

}
