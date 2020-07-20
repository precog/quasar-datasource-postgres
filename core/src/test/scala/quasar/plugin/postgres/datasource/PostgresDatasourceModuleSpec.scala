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

import cats.effect._
import cats.implicits._

import quasar.{EffectfulQSpec, RateLimiter, NoopRateLimitUpdater, RateLimiting}
import quasar.api.datasource.DatasourceError
import quasar.api.resource.ResourcePath
import quasar.connector.{ByteStore, ResourceError}
import quasar.contrib.scalaz.MonadError_

import java.util.UUID
import scala.concurrent.ExecutionContext.Implicits.global

import shims._

object PostgresDatasourceModuleSpec extends EffectfulQSpec[IO] {

  implicit val ioContextShift: ContextShift[IO] =
    IO.contextShift(global)

  implicit val ioTimer: Timer[IO] =
    IO.timer(global)

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val rateLimiting: IO[RateLimiting[IO, UUID]] =
    RateLimiter(1.0, IO.delay(UUID.randomUUID()), NoopRateLimitUpdater[IO, UUID])

  "initialization" should {
    "fail with malformed config when not decodable" >>* {
      val cfg = Json("malformed" := true)

      rateLimiting.flatMap(rl =>
        PostgresDatasourceModule.lightweightDatasource[IO, UUID](cfg, rl, ByteStore.void[IO]) use {
          case Left(DatasourceError.MalformedConfiguration(_, c, _)) =>
            IO.pure(c must_=== jString(Redacted))

          case _ => ko("Expected a malformed configuration").pure[IO]
        })
    }

    "fail when unable to connect to database" >>* {
      val cfg = Json("connectionUri" := "postgresql://localhost:1234/foobar?user=alice&password=secret")

      rateLimiting.flatMap(rl =>
        PostgresDatasourceModule.lightweightDatasource[IO, UUID](cfg, rl, ByteStore.void[IO]) use {
          case Left(DatasourceError.ConnectionFailed(_, c, _)) =>
            IO.pure(c.some must_=== cfg.as[Config].map(_.sanitized.asJson).toOption)

          case _ => ko("Expected a connection failed").pure[IO]
        })
    }

    "succeeds with a valid config" >>* {
      val cfg = Json("connectionUri" := "postgresql://localhost:54322/postgres?user=postgres&password=postgres")

      rateLimiting.flatMap(rl =>
        PostgresDatasourceModule.lightweightDatasource[IO, UUID](cfg, rl, ByteStore.void[IO]) use {
          case Right(ds) =>
            ds.prefixedChildPaths(ResourcePath.root())
              .use(_.sequence.unNone.compile.toList)
              .map(_ must not(beEmpty))

          case _ => ko("Expected connection to succeed").pure[IO]
        })
    }
  }

  "config migration" should {
    "migrate config as itself" >> {
      val config = Json("connectionUri" := "postgresql://localhost:54322/postgres?user=postgres&password=postgres", "connectionPoolSize" := 12)

      PostgresDatasourceModule.migrateConfig[IO](config).unsafeRunSync() must beRight(config)
    }

    "fail to migrate malformed config" >> {
      val malformed = "malformed".asJson

      val error = DatasourceError.MalformedConfiguration(
        PostgresDatasourceModule.kind,
        Json(),
        "Configuration to migrate is malformed.")

      PostgresDatasourceModule.migrateConfig[IO](malformed).unsafeRunSync() must beLeft(error)
    }
  }
}
