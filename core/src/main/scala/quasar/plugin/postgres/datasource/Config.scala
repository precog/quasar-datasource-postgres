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

import java.net.URI
import java.util.Properties

import quasar.api.datasource.{DatasourceError, DatasourceType}
import quasar.api.datasource.DatasourceError.InvalidConfiguration
import quasar.plugin.postgres.datasource.PostgresCodecs._

import org.postgresql.Driver

import scalaz.NonEmptyList

final case class Config(connectionUri: URI, connectionPoolSize: Option[Int]) {
  import Config._

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def sanitized: Config = {
    copy(connectionUri = Sanitization.sanitizeURI(connectionUri))
  }

  def properties: Option[Properties] = {
    val ops = Option(Driver.parseURL(s"jdbc:${connectionUri.toString}", null))

    ops map { ps =>
      ps.setProperty(
        DefaultRowFetchSizeKey,
        ps.getProperty(DefaultRowFetchSizeKey, DefaultRowFetchSize.toString()))
    }

    ops
  }

  def reconfigureNonSensitive(patch: PatchConfig, kind: DatasourceType)
      :Either[InvalidConfiguration[PatchConfig], Config] = {
    if(patch.isSensitive){
      Left(DatasourceError.InvalidConfiguration[PatchConfig](
        kind,
        patch.sanitized,
        NonEmptyList("Target configuration contains sensitive information.")))
    } else {
      Right(this.copy(
        connectionPoolSize = Some(patch.connectionPoolSize)))
    }
  }

  def poolSize: Int =
    connectionPoolSize getOrElse DefaultConnectionPoolSize
}

object Config {
  val DefaultRowFetchSizeKey: String = "defaultRowFetchSize"
  val DefaultRowFetchSize: Int = 64
  // The default maximum number of database connections per-datasource.
  val DefaultConnectionPoolSize: Int = 10

  implicit val codecJson: CodecJson[Config] = {
    casecodec2(Config.apply, Config.unapply)("connectionUri", "connectionPoolSize")
  }
}
