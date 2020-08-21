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

import quasar.plugin.postgres.datasource.PostgresCodecs._

import argonaut._, Argonaut._

import java.net.URI

final case class PatchConfig(connectionPoolSize: Int, connectionURI: Option[URI]) {
  
  def sanitized: PatchConfig = connectionURI match {
    case Some(uri) => copy(connectionURI = Some(Sanitization.sanitizeURI(uri)))
    case None => this
  }

  def isSensitive: Boolean = connectionURI.isDefined
}

object PatchConfig {
  implicit val codecJson: CodecJson[PatchConfig] = { 

    casecodec2(PatchConfig.apply, PatchConfig.unapply)("connectionPoolSize", "connectionUri")
  }
}
