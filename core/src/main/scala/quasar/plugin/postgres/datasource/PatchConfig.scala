

package quasar.plugin.postgres.datasource

import slamdata.Predef._

import argonaut._, Argonaut._

import cats.implicits._

import java.net.URI

import scala.util.control.NonFatal

import quasar.plugin.postgres.datasource.PostgresCodecs._

final case class PatchConfig(connectionPoolSize: Int, connectionURI: Option[URI]){

  def isSensative: Boolean = this.connectionURI match {
    case Some(x) => true
    case None => false
  } 

}

object PatchConfig{
  implicit val codecJson: CodecJson[PatchConfig] ={

    casecodec2(PatchConfig.apply, PatchConfig.unapply)("connectionPoolSize", "connectionUri")
  }
}
