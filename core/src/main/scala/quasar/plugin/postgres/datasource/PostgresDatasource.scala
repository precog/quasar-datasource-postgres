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

import java.time.format.DateTimeFormatter

import cats.~>
import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._
import doobie.util.log.{ExecFailure, ProcessingFailure, Success}

import fs2.{text, Pull, Stream}

import org.slf4s.Logging

import quasar.{ScalarStage, ScalarStages}
import quasar.api.ColumnType
import quasar.api.DataPathSegment
import quasar.api.datasource.DatasourceType
import quasar.api.resource.{ResourcePathType => RPT, _}
import quasar.api.push.InternalKey
import quasar.common.CPathField
import quasar.connector.{ResourceError => RE, Offset, _}
import quasar.connector.datasource.{BatchLoader, DatasourceModule, Loader}
import quasar.qscript.InterpretedRead
import quasar.connector.datasource.Loader

import shims._

final class PostgresDatasource[F[_]: MonadResourceErr: Sync](
    xa: Transactor[F])
    extends DatasourceModule.DS[F]
    with Logging {

  import PostgresDatasource._

  val kind: DatasourceType = PostgresDatasourceModule.kind

  val loaders =
    NonEmptyList.one(Loader.Batch(BatchLoader.Seek(load(_, _))))

  def pathIsResource(path: ResourcePath): Resource[F, Boolean] =
    Resource.eval(pathToLoc(path) match {
      case Some(Right((schema, table))) =>
        tableExists(schema, table).transact(xa)

      case _ => false.pure[F]
    })

  def prefixedChildPaths(prefixPath: ResourcePath): Resource[F, Option[Stream[F, (ResourceName, RPT.Physical)]]] =
    if (prefixPath === ResourcePath.Root)
      allSchemas
        .map(n => (ResourceName(n), RPT.prefix))
        .transact(xa)
        .some
        .pure[Resource[F, ?]]
    else
      pathToLoc(prefixPath) match {
        case Some(Right((schema, table))) =>
          Resource.eval(
            tableExists(schema, table)
              .map(p => if (p) Some(Stream.empty.covaryAll[F, (ResourceName, RPT.Physical)]) else None)
              .transact(xa))

        case Some(Left(schema)) =>
          val l = Some(schema).filterNot(containsWildcard).map(Left(_))

          val paths =
            tables(l)
              .filter(_.schemaName === schema)
              .map(m => (ResourceName(m.tableName), RPT.leafResource))
              .pull.peek1
              .flatMap(t => Pull.output1(t.map(_._2)))
              .stream

          for {
            c <- xa.connect(xa.kernel)
            opt <- paths.translate(runCIO(c)).compile.resource.lastOrError
          } yield opt.map(_.translate(runCIO(c)))

        case None => (None: Option[Stream[F, (ResourceName, RPT.Physical)]]).pure[Resource[F, ?]]
      }

  ////

  private def load(read: InterpretedRead[ResourcePath], offset: Option[Offset]): Resource[F, QueryResult[F]] =
    pathToLoc(read.path) match {
      case Some(Right((schema, table))) =>
        val back = tableExists(schema, table) map { exists =>
          if (exists)
            Right(maskedColumns(read.stages) match {
              case Some((columns, nextStages)) =>
                (tableAsJsonBytes(schema, ColumnProjections.Explicit(columns), table, offset.flatMap(offsetProjection)), nextStages)

              case None =>
                (tableAsJsonBytes(schema, ColumnProjections.All, table, offset.flatMap(offsetProjection)), read.stages)
            })
          else
            Left(RE.pathNotFound[RE](read.path))
        }

        xa.connect(xa.kernel)
          .evalMap(c => runCIO(c)(back.map(_.map(_.leftMap(_.translate(runCIO(c)))))))
          .evalMap {
            case Right((s, stages)) =>
              QueryResult.typed(DataFormat.ldjson, ResultData.Continuous(s), stages).pure[F]

            case Left(re) =>
              MonadResourceErr[F].raiseError[QueryResult[F]](re)
          }

      case _ =>
        Resource.eval(MonadResourceErr[F].raiseError(RE.notAResource(read.path)))
    }

  private def offsetProjection(offset: Offset): Option[(Ident, String)] =
    offset match {
      case Offset.Internal(NonEmptyList(DataPathSegment.Field(proj), Nil), value) => {
        val k: InternalKey.Actual[A] forSome { type A } = value.value

        val offsetValue = k match {
          case InternalKey.RealKey(num) => Some(num.toString)
          case InternalKey.StringKey(str) => Some(str)
          case InternalKey.DateTimeKey(dt) => Some(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(dt))
          case InternalKey.DateKey(d) => None
          case InternalKey.LocalDateKey(ld) => Some(DateTimeFormatter.ISO_LOCAL_DATE.format(ld))
          case InternalKey.LocalDateTimeKey(ldt) => Some(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(ldt))
        }

        offsetValue.map((proj, _))
      }
      case _ =>
        None
    }

  private type Loc = Option[Either[Schema, (Schema, Table)]]
  private type Predicate = Option[(Ident, String)]

  // Characters considered as pattern placeholders in
  // `DatabaseMetaData#getTables`
  private val Wildcards: Set[Char] = Set('_', '%')

  // Log doobie queries at DEBUG
  private val logHandler: LogHandler =
    LogHandler {
      case Success(q, _, e, p) =>
        log.debug(s"SUCCESS: `$q` in ${(e + p).toMillis} ms (${e.toMillis} ms exec, ${p.toMillis} ms proc)")

      case ExecFailure(q, _, e, t) =>
        log.debug(s"EXECUTION_FAILURE: `$q` after ${e.toMillis} ms, detail: ${t.getMessage}", t)

      case ProcessingFailure(q, _, e, p, t) =>
        log.debug(s"PROCESSING_FAILURE: `$q` after ${(e + p).toMillis} ms (${e.toMillis} ms exec, ${p.toMillis} ms proc (failed)), detail: ${t.getMessage}", t)
    }

  // We use `tables` here to constrain discovered schemas
  // to be only those containing visible tables.
  private def allSchemas: Stream[ConnectionIO, String] =
    tables(None)
      .scan((Set.empty[String], None: Option[String])) {
        case ((seen, _), TableMeta(schema, _)) =>
          if (seen(schema))
            (seen, None)
          else
            (seen + schema, Some(schema))
      }
      .map(_._2)
      .unNone

  /** Returns a `Loc` representing the most specific tables selector.
    *
    * To be a tables selector, an `Ident` must not contain any of the wildcard characters.
    */
  private def locSelector(schema: Schema, table: Table): Loc =
    (containsWildcard(schema), containsWildcard(table)) match {
      case (true, _) => None
      case (false, true) => Some(Left(schema))
      case (false, false) => Some(Right((schema, table)))
    }

  private def containsWildcard(s: String): Boolean =
    s.exists(Wildcards)

  /** Returns a quoted and escaped version of `ident`. */
  private def hygienicIdent(ident: Ident): Ident =
    s""""${ident.replace("\"", "\"\"")}""""

  private def maskedColumns: ScalarStages => Option[(NonEmptyList[String], ScalarStages)] = {
    case ss @ ScalarStages(idStatus, ScalarStage.Mask(m) :: t) =>
      for {
        paths <- m.keySet.toList.toNel

        fields0 <- paths.traverse(_.head collect { case CPathField(f) => f })
        fields = fields0.distinct

        eliminated = m forall {
          case (p, t) => p.tail.nodes.isEmpty && t === ColumnType.Top
        }
      } yield (fields, if (eliminated) ScalarStages(idStatus, t) else ss)

    case _ => None
  }

  private def pathToLoc(rp: ResourcePath): Loc =
    Some(rp) collect {
      case schema /: ResourcePath.Root => Left(schema)
      case schema /: table /: ResourcePath.Root => Right((schema, table))
    }

  private def runCIO(c: java.sql.Connection): ConnectionIO ~> F =
    λ[ConnectionIO ~> F](_.foldMap(xa.interpret).run(c))

  private def tableAsJsonBytes(schema: Schema, columns: ColumnProjections, table: Table, predicate: Predicate)
      : Stream[ConnectionIO, Byte] = {

    val schemaFr = Fragment.const0(hygienicIdent(schema))
    val tableFr = Fragment.const(hygienicIdent(table))
    val locFr = schemaFr ++ fr0"." ++ tableFr

    val predicateFr = predicate match {
      case Some((column, value)) => {
        val col = hygienicIdent(column)

        fr"WHERE $col >= $value"
      }
      case None =>
        fr""
    }

    val fromFr = columns match {
      case ColumnProjections.Explicit(cs) =>
        val colsFr = cs.map(n => Fragment.const(hygienicIdent(n))).intercalate(fr",")
        Fragments.parentheses(fr"SELECT" ++ colsFr ++ fr"FROM" ++ locFr ++ predicateFr)

      case ColumnProjections.All =>
        Fragments.parentheses(fr"SELECT * FROM" ++ locFr ++ predicateFr)
    }

    val sql =
      fr"SELECT to_json(t) FROM" ++ fromFr ++ fr0"AS t"

    sql.queryWithLogHandler[String](logHandler)
      .stream
      .intersperse("\n")
      .through(text.utf8Encode)
  }

  private def tableExists(schema: Schema, table: Table): ConnectionIO[Boolean] =
    tables(locSelector(schema, table))
      .exists(m => m.schemaName === schema && m.tableName === table)
      .compile
      .lastOrError

  private def tables(selector: Loc): Stream[ConnectionIO, TableMeta] = {
    val (selectSchema, selectTable) = selector match {
      case Some(Left(s)) => (s, "%")
      case Some(Right((s, t))) => (s, t)
      case None => (null, "%")
    }

    Stream.force(for {
      catalog <- HC.getCatalog
      rs <- HC.getMetaData(FDMD.getTables(catalog, selectSchema, selectTable, VisibleTableTypes))
      ts = HRS.stream[TableMeta](MetaChunkSize)
    } yield ts.translate(λ[ResultSetIO ~> ConnectionIO](FC.embed(rs, _))))
  }
}

object PostgresDatasource {

  /** The chunk size used for metadata streams. */
  val MetaChunkSize: Int = 1024

  /** The types of tables that should be visible during discovery.
    *
    * Available types from DatabaseMetaData#getTableTypes
    *
    * FOREIGN TABLE, INDEX, MATERIALIZED VIEW, SEQUENCE, SYSTEM INDEX,
    * SYSTEM TABLE, SYSTEM TOAST INDEX, SYSTEM TOAST TABLE, SYSTEM VIEW,
    * TABLE, TEMPORARY INDEX, TEMPORARY SEQUENCE, TEMPORARY TABLE,
    * TEMPORARY VIEW, TYPE, VIEW
    */
  val VisibleTableTypes: Array[String] =
    Array(
      "FOREIGN TABLE",
      "MATERIALIZED VIEW",
      "SYSTEM TABLE",
      "SYSTEM TOAST TABLE",
      "SYSTEM VIEW",
      "TABLE",
      "TEMPORARY TABLE",
      "TEMPORARY VIEW",
      "VIEW")

  def apply[F[_]: MonadResourceErr: Sync](
      xa: Transactor[F])
      : DatasourceModule.DS[F] =
    new PostgresDatasource(xa)
}
