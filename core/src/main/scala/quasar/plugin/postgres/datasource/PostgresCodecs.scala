package quasar.plugin.postgres.datasource

import slamdata.Predef._

import argonaut._, Argonaut._

import cats.implicits._

import java.net.URI

import scala.util.control.NonFatal


object PostgresCodecs {

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
}
