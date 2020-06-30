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


  def reconfigureNonSensitive(patch: Config[URI, Option[Int]], kind: DatasourceType)
    :Either[InvalidConfiguration[Config[URI, Option[Int]]], Config[URI, Option[Int]]]
    {
      if(patch.isSensative)
      {
        Left(DatasourceError.InvalidConfiguration[Config[URI, Option[Int]]](
          kind,
          patch.sanitize,
          "Target configuration contains sensitive information."))
      } else {
          Right(self.copy(
              connectionPoolSize = patch.connectionPoolSize
            ))
      }
    }

  def isSensitive: Boolean = _ match {
    case Some(x) => connectionURI != null //is there a better way to check the URI?
    case None => false

  }

}

object Config {
  implicit val codecJson: CodecJson[Config] = {
    implicit val uriDecodeJson: DecodeJson[URI] =
      DecodeJson(c => c.as[String] flatMap { s =>
        try {
          DecodeResult.ok(new URI(s))
        } catch {
          case NonFatal(t) => DecodeResult.fail("URI", c.history)
        }
      })

    implicit val uriEncodeJson: EncodeJson[URI] =
      EncodeJson.of[String].contramap(_.toString)

    casecodec2(Config.apply, Config.unapply)("connectionUri", "connectionPoolSize")
  }
}
