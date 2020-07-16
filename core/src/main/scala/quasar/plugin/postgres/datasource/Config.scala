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

import cats.implicits._

import java.net.URI

import scala.util.control.NonFatal

import quasar.api.datasource.{DatasourceError, DatasourceType}

import quasar.api.datasource.DatasourceError.InvalidConfiguration

import scalaz.NonEmptyList

import quasar.plugin.postgres.datasource.PostgresCodecs._

final case class Config(connectionUri: URI, connectionPoolSize: Option[Int]) {
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def sanitized: Config = {
    val sanitizedUserInfo =
      Option(connectionUri.getUserInfo) map { ui =>
        val colon = ui.indexOf(':')

        if (colon === -1)
          ui
        else
          ui.substring(0, colon) + s":${Redacted}"
      }

    val sanitizedQuery =
      Option(connectionUri.getQuery) map { q =>
        val pairs = q.split('&').toList map { kv =>
          if (kv.toLowerCase.startsWith("password"))
            s"password=${Redacted}"
          else if (kv.toLowerCase.startsWith("sslpassword"))
            s"sslpassword=${Redacted}"
          else
            kv
        }

        pairs.intercalate("&")
      }

    copy(connectionUri = new URI(
      connectionUri.getScheme,
      sanitizedUserInfo.orNull,
      connectionUri.getHost,
      connectionUri.getPort,
      connectionUri.getPath,
      sanitizedQuery.orNull,
      connectionUri.getFragment))
  }


  def reconfigureNonSensitive(patch: PatchConfig, kind: DatasourceType)
    :Either[InvalidConfiguration[Config], Config] = {
      if(patch.isSensative)
      {

        Left(DatasourceError.InvalidConfiguration[Config](
          kind,
          patch.sanitize,
          NonEmptyList("Target configuration contains sensitive information.")))
      } else {
          Right(this.copy(
              connectionPoolSize = Option{patch.connectionPoolSize}
            ))
      }
    }
}

object Config {
  implicit val codecJson: CodecJson[Config] = {
    casecodec2(Config.apply, Config.unapply)("connectionUri", "connectionPoolSize")
  }
}
