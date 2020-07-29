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

//import argonaut._, Argonaut._

import cats.implicits._

import java.net.URI

//import scala.util.control.NonFatal




object Sanitization{

  def sanitizeURI(uri: URI): URI = {
        val sanitizedUserInfo =
          Option(uri.getUserInfo) map { ui =>
           val colon = ui.indexOf(':')

           if (colon === -1)
             ui
           else
              ui.substring(0, colon) + s":${Redacted}"
          }

       val sanitizedQuery =
          Option(uri.getQuery) map { q =>
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

        new URI(
         uri.getScheme,
         sanitizedUserInfo.orNull,
         uri.getHost,
         uri.getPort,
         uri.getPath,
         sanitizedQuery.orNull,
         uri.getFragment)
      }
}
