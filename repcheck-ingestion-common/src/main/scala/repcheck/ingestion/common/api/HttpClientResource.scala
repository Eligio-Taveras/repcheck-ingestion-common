package repcheck.ingestion.common.api

import cats.effect.{Async, Resource}

import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder

import fs2.io.net.Network

object HttpClientResource {

  def make[F[_]: Async: Network](config: HttpClientConfig): Resource[F, Client[F]] =
    EmberClientBuilder
      .default[F]
      .withTimeout(config.requestTimeout)
      .withIdleConnectionTime(config.idleTimeout)
      .withMaxTotal(config.maxTotalConnections)
      .build

}
