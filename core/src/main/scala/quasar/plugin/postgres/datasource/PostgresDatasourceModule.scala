/*
 * Copyright 2020 Precog Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.plugin.postgres.datasource

import slamdata.Predef._

import argonaut._, Argonaut._

import cats.data._
import cats.effect._
import cats.kernel.Hash
import cats.implicits._

import doobie._
import doobie.hikari.HikariTransactor
import doobie.implicits._

import java.util.{Properties, UUID}
import java.util.concurrent.Executors

import org.slf4s.Logging

import quasar.RateLimiting
import quasar.api.datasource.{DatasourceError => DE, DatasourceType}
import quasar.concurrent._
import quasar.connector.{ByteStore, MonadResourceErr, ExternalCredentials}
import quasar.connector.datasource.{DatasourceModule, Reconfiguration}


import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NonFatal

import scalaz.NonEmptyList

object PostgresDatasourceModule extends DatasourceModule with Logging {

  type InitErr = DE.InitializationError[Json]

  // The duration to await validation of the initial connection.
  val ValidationTimeout: FiniteDuration = 10.seconds

  val kind: DatasourceType = DatasourceType("postgres", 1L)

  def sanitizeConfig(config: Json): Json =
    config.as[Config]
      .map(_.sanitized.asJson)
      .getOr(jEmptyObject)

  def sanitizePatch(config: Json): Json =
    config.as[PatchConfig]
      .map(_.sanitized.asJson)
      .getOr(jEmptyObject)

  def reconfigure(original: Json, patch: Json): Either[DE.ConfigurationError[Json], (Reconfiguration, Json)] = {
    val back = for{
      org <- original.as[Config].result match {
        case Left(_) =>
          Left(DE.MalformedConfiguration[Json](
            kind,
            sanitizeConfig(original),
            "Source configuration in reconfiguration is malformed."))
        case Right(x) => Right(x)
      }
      pat <- patch.as[PatchConfig].result match {
        case Left(_) => Left(DE.MalformedConfiguration[Json](
          kind,
          sanitizePatch(patch),
          "Target configuration in reconfiguration is malformed."))
        case Right(x) => Right(x)
      }

      reconfigured <- org.reconfigureNonSensitive(pat, kind) match {
        case Left(err) => Left(err.copy(config = err.config.asJson))
        case Right(cfg) => Right(cfg.asJson)
      }
    } yield reconfigured
    back.tupleLeft(Reconfiguration.Reset)
  }

  def migrateConfig[F[_]: Sync](from: Long, to: Long, config: Json): F[Either[DE.ConfigurationError[Json], Json]] =
    Sync[F].pure(Right(config))

  def datasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer, A: Hash](
      config: Json,
      rateLimiter: RateLimiting[F, A],
      byteStore: ByteStore[F],
      getAuth: UUID => F[Option[ExternalCredentials[F]]])(
      implicit ec: ExecutionContext)
      : Resource[F, Either[InitErr, DatasourceModule.DS[F]]] = {

    val cfg0: Either[InitErr, Config] =
      config.as[Config].fold(
        (err, c) =>
          Left(DE.malformedConfiguration[Json, InitErr](
            kind,
            jString(Redacted),
            err)),

        Right(_))

    val validateConnection: ConnectionIO[Either[InitErr, Unit]] =
      FC.isValid(ValidationTimeout.toSeconds.toInt) map { v =>
        if (!v) Left(connectionInvalid(sanitizeConfig(config))) else Right(())
      }

    lazy val invalidConfig =
      DE.invalidConfiguration[Json, InitErr](
        kind,
        jString(Redacted),
        NonEmptyList("Provided PostgreSQL JDBC URL is invalid"))

    val init = for {
      cfg <- EitherT(cfg0.pure[Resource[F, ?]])

      suffix <- EitherT.right(Resource.eval(Sync[F].delay(Random.alphanumeric.take(6).mkString)))

      awaitPool <- EitherT.right(awaitConnPool[F](s"pgsrc-await-$suffix", cfg.poolSize))

      xaPool <- EitherT.right(Blocker.cached[F](s"pgsrc-transact-$suffix"))

      props <- EitherT.fromOption[Resource[F, *]](cfg.properties, invalidConfig)

      xa <- EitherT.right(hikariTransactor[F](cfg, props, awaitPool, xaPool))

      _ <- EitherT(Resource.eval(validateConnection.transact(xa) recover {
        case NonFatal(ex: Exception) =>
          Left(DE.connectionFailed[Json, InitErr](kind, sanitizeConfig(config), ex))
      }))

      _ <- EitherT.right[InitErr](Resource.eval(Sync[F].delay(
        log.info(s"Initialized postgres datasource: tag = $suffix, config = ${cfg.sanitized.asJson}"))))

    } yield new PostgresDatasource(xa): DatasourceModule.DS[F]

    init.value
  }

  ////

  private def awaitConnPool[F[_]](name: String, size: Int)(implicit F: Sync[F])
      : Resource[F, ExecutionContext] = {

    val alloc =
      F.delay(Executors.newFixedThreadPool(size, NamedDaemonThreadFactory(name)))

    Resource.make(alloc)(es => F.delay(es.shutdown()))
      .map(ExecutionContext.fromExecutor)
  }

  private def connectionInvalid(c: Json): InitErr =
    DE.connectionFailed[Json, InitErr](
      kind, c, new RuntimeException("Connection is invalid."))

  private def hikariTransactor[F[_]: Async: ContextShift](
      config: Config,
      properties: Properties,
      connectPool: ExecutionContext,
      xaBlocker: Blocker)
      : Resource[F, HikariTransactor[F]] = {

    HikariTransactor.initial[F](connectPool, xaBlocker) evalMap { xa =>
      xa.configure { ds =>
        Sync[F] delay {
          ds.setJdbcUrl(s"jdbc:${config.connectionUri}")
          ds.setDriverClassName(PostgresDriverFqcn)
          ds.setMaximumPoolSize(config.poolSize)
          // This is needed for `defaultRowFetchSize` to work in some situations
          ds.setAutoCommit(false)
          ds.setDataSourceProperties(properties)
          xa
        }
      }
    }
  }
}
