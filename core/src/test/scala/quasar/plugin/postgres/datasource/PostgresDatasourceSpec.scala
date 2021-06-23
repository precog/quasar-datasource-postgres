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

import argonaut._, Argonaut._, JawnParser._

import cats.~>
import cats.effect._
import cats.implicits._

import doobie._
import doobie.implicits._

import fs2.Stream

import jawnfs2._

import org.specs2.specification.BeforeAfterAll

import quasar.{IdStatus, ScalarStage, ScalarStages}
import quasar.api.ColumnType
import quasar.api.resource.{ResourcePathType => RPT, _}
import quasar.common.CPath
import quasar.concurrent.unsafe._
import quasar.connector.{ResourceError => RE, _}
import quasar.connector.datasource.{DatasourceSpec, LightweightDatasourceModule}
import quasar.contrib.scalaz.MonadError_
import quasar.qscript.InterpretedRead

import scala.concurrent.ExecutionContext.Implicits.global

import shims.applicativeToScalaz

object PostgresDatasourceSpec
    extends DatasourceSpec[IO, Stream[IO, ?], RPT.Physical]
    with BeforeAfterAll {

  // Required to avoid examples observing each others' database changes
  // should be able to remove once
  //
  // https://app.clubhouse.io/data/story/9544/improve-datasourcespec-by-representing-the-datasource-under-test-as-a-cats-effect-resource
  //
  // is implemented.
  sequential

  // Uncomment to log all queries to STDOUT
  //implicit val doobieLogHandler: LogHandler = LogHandler.jdkLogHandler

  implicit val ioContextShift: ContextShift[IO] =
    IO.contextShift(global)

  implicit val ioMonadResourceErr: MonadError_[IO, RE] =
    MonadError_.facet[IO](RE.throwableP)

  val xaBlocker = Blocker.unsafeCached("postgres-datasource-spec")

  val xa = Transactor.fromDriverManager[IO](
    PostgresDriverFqcn,
    "jdbc:postgresql://localhost:54322/postgres?user=postgres&password=postgres",
    xaBlocker)

  val pgds: LightweightDatasourceModule.DS[IO] = PostgresDatasource[IO](xa)

  val datasource = pgds.pure[Resource[IO, ?]]

  val nonExistentPath = ResourcePath.root() / ResourceName("schemadne") / ResourceName("tabledne")

  def gatherMultiple[A](s: Stream[IO, A]) = s.compile.toList

  def beforeAll(): Unit = {
    val setup = for {
      _ <- sql"""CREATE SCHEMA "pgsrcSchemaA"""".update.run
      _ <- sql"""CREATE SCHEMA "pgsrcSchemaB"""".update.run

      _ <- sql"""CREATE TABLE "pgsrcSchemaA"."tableA1" (x SMALLINT, y SMALLINT)""".update.run
      _ <- sql"""CREATE TABLE "pgsrcSchemaA"."tableA2" (a TEXT, b INT)""".update.run

      _ <- sql"""CREATE TABLE "pgsrcSchemaB"."tableB1" (foo VARCHAR, bar BOOLEAN)""".update.run
      _ <- sql"""CREATE TABLE "pgsrcSchemaB"."tableB2" (ts TIMESTAMP)""".update.run

      _ <- sql"""CREATE TABLE "pgsrcPublic1" (name VARCHAR, age INT)""".update.run
    } yield ()

    setup.transact(xa).unsafeRunSync()
  }

  def afterAll(): Unit = {
    val teardown = for {
      _ <- sql"""DROP SCHEMA "pgsrcSchemaA" CASCADE""".update.run
      _ <- sql"""DROP SCHEMA "pgsrcSchemaB" CASCADE""".update.run
      _ <- sql"""DROP TABLE "pgsrcPublic1"""".update.run
    } yield ()

    teardown.transact(xa).unsafeRunSync()
  }

  implicit class DatasourceOps(val ds: LightweightDatasourceModule.DS[IO]) extends scala.AnyVal {
    def evaluate(ir: InterpretedRead[ResourcePath]) =
      ds.loadFull(ir) getOrElseF Resource.eval(IO.raiseError(new RuntimeException("No batch loader!")))
  }

  "schema handling" >> {
    "public tables are resources under /public, by default" >>* {
      pgds
        .pathIsResource(ResourcePath.root() / ResourceName("public") / ResourceName("pgsrcPublic1"))
        .use(b => IO.pure(b must beTrue))
    }

    "tables in created schemas are resources" >>* {
      pgds
        .pathIsResource(ResourcePath.root() / ResourceName("pgsrcSchemaB") / ResourceName("tableB1"))
        .use(b => IO.pure(b must beTrue))
    }
  }

  "naming" >> {
    def mustBeAResource(schema: String, table: String) = {
      val setup = for {
        _ <- (fr"CREATE SCHEMA" ++ Fragment.const0(s""""$schema"""")).update.run
        _ <- (fr"CREATE TABLE" ++ Fragment.const(s""""$schema"."$table"""") ++ fr0"(value varchar)").update.run
      } yield ()

      val teardown =
        (fr"DROP SCHEMA" ++ Fragment.const(s""""$schema"""") ++ fr0"CASCADE").update.run

      xa.connect(xa.kernel)
        .flatMap(c =>
          Resource.make(setup)(_ => teardown.void)
            .mapK(λ[ConnectionIO ~> IO](_.foldMap(xa.interpret).run(c))))
        .flatMap(_ => pgds.pathIsResource(ResourcePath.root() / ResourceName(schema) / ResourceName(table)))
        .use(b => IO.pure(b must beTrue))
    }

    "support schema names containing '_'" >>* mustBeAResource("some_schema_x", "tablex")

    "support schema names containing '%'" >>* mustBeAResource("some%schema%y", "tabley")

    "support table names containing '_'" >>* mustBeAResource("someschemau", "table_q")

    "support table names containing '%'" >>* mustBeAResource("someschemav", "table%r")
  }

  "evaluation" >> {
    "path with length 0 fails with not a resource" >>* {
      val ir = InterpretedRead(ResourcePath.root(), ScalarStages.Id)

      MonadResourceErr.attempt(pgds.evaluate(ir).use(_ => IO.unit))
        .map(_.toEither must beLeft(RE.notAResource(ir.path)))
    }

    "path with length 1 fails with not a resource" >>* {
      val ir = InterpretedRead(
        ResourcePath.root() / ResourceName("a"),
        ScalarStages.Id)

      MonadResourceErr.attempt(pgds.evaluate(ir).use(_ => IO.unit))
        .map(_.toEither must beLeft(RE.notAResource(ir.path)))
    }

    "path with length 3 fails with not a resource" >>* {
      val ir = InterpretedRead(
        ResourcePath.root() / ResourceName("x") / ResourceName("y") / ResourceName("z"),
        ScalarStages.Id)

      MonadResourceErr.attempt(pgds.evaluate(ir).use(_ => IO.unit))
        .map(_.toEither must beLeft(RE.notAResource(ir.path)))
    }

    "path with length 2 that isn't a table fails with path not found" >>* {
      val ir = InterpretedRead(nonExistentPath, ScalarStages.Id)

      MonadResourceErr.attempt(pgds.evaluate(ir).use(_ => IO.unit))
        .map(_.toEither must beLeft(RE.pathNotFound(ir.path)))
    }

    "path to extant empty table returns empty results" >>* {
      val ir = InterpretedRead(
        ResourcePath.root() / ResourceName("pgsrcSchemaA") / ResourceName("tableA1"),
        ScalarStages.Id)

      pgds.evaluate(ir) use {
        case QueryResult.Typed(fmt, bs, stages) =>
          fmt must_=== DataFormat.ldjson
          stages must_=== ScalarStages.Id
          bs.data.compile.toList map (_ must beEmpty)

        case _ => IO.pure(ko("Expected QueryResult.Typed"))
      }
    }

    "path to extant non-empty table returns rows as line-delimited json" >>* {
      val widgets = List(
        Widget("X349", 34.23, 12.5),
        Widget("XY34", 64.25, 23.1),
        Widget("CR12", 0.023, 40.33))

      for {
        _ <- loadWidgets(widgets)
        (_, ws) <- resultsOf[Widget](widgetsRead())
      } yield ws must containTheSameElementsAs(widgets)
    }
  }

  "mask pushdown" >> {
    val maskWidgets = List(
      Widget("A", 1.1, 2.2),
      Widget("B", 3.3, 4.4),
      Widget("C", 5.5, 6.6),
      Widget("D", 7.7, 8.8))

    "projects when all masked paths begin with a field" >>* {
      val mask: ScalarStage = ScalarStage.Mask(Map(
        CPath.parse(".serial") -> Set(ColumnType.String),
        CPath.parse(".height") -> Set(ColumnType.Number)))

      val expected = List(
        Json("serial" := "A", "height" := 2.2),
        Json("serial" := "B", "height" := 4.4),
        Json("serial" := "C", "height" := 6.6),
        Json("serial" := "D", "height" := 8.8))

      for {
        _ <- loadWidgets(maskWidgets)
        (_, ws) <- resultsOf[Json](widgetsRead(ScalarStages(IdStatus.ExcludeId, List(mask))))
      } yield ws must containTheSameElementsAs(expected)
    }

    "noop when a masked path doesn't begin with a field" >>* {
      val mask: ScalarStage = ScalarStage.Mask(Map(
        CPath.parse("[2].serial") -> Set(ColumnType.String),
        CPath.parse("[2].height") -> Set(ColumnType.Number)))

      val stages = ScalarStages(IdStatus.ExcludeId, List(mask))

      for {
        _ <- loadWidgets(maskWidgets)
        r <- resultsOf[Widget](widgetsRead(stages))
      } yield r must (stages, maskWidgets).zip(be_===, containTheSameElementsAs(_))
    }

    "noop when mask stage isn't first" >>* {
      val mask: ScalarStage = ScalarStage.Mask(Map(
        CPath.parse(".foo.serial") -> Set(ColumnType.String),
        CPath.parse(".foo.height") -> Set(ColumnType.Number)))

      val stages = ScalarStages(IdStatus.ExcludeId, List(ScalarStage.Wrap("foo"), mask))

      for {
        _ <- loadWidgets(maskWidgets)
        r <- resultsOf[Widget](widgetsRead(stages))
      } yield r must (stages, maskWidgets).zip(be_===, containTheSameElementsAs(_))
    }

    "eliminates mask stage when all paths are a single field and type is top" >>* {
      val mask: ScalarStage = ScalarStage.Mask(Map(
        CPath.parse(".width") -> ColumnType.Top,
        CPath.parse(".height") -> ColumnType.Top))

      val expected = List(
        Json("width" := 1.1, "height" := 2.2),
        Json("width" := 3.3, "height" := 4.4),
        Json("width" := 5.5, "height" := 6.6),
        Json("width" := 7.7, "height" := 8.8))

      for {
        _ <- loadWidgets(maskWidgets)
        r <- resultsOf[Json](widgetsRead(ScalarStages(IdStatus.ExcludeId, List(mask))))
      } yield r must (ScalarStages.Id, expected).zip(be_===, containTheSameElementsAs(_))
    }

    "retains mask stage when any path has length > 1" >>* {
      val mask: ScalarStage = ScalarStage.Mask(Map(
        CPath.parse(".width[2]") -> ColumnType.Top,
        CPath.parse(".height") -> ColumnType.Top))

      val stages = ScalarStages(IdStatus.ExcludeId, List(mask))

      val expected = List(
        Json("width" := 1.1, "height" := 2.2),
        Json("width" := 3.3, "height" := 4.4),
        Json("width" := 5.5, "height" := 6.6),
        Json("width" := 7.7, "height" := 8.8))

      for {
        _ <- loadWidgets(maskWidgets)
        r <- resultsOf[Json](widgetsRead(stages))
      } yield r must (stages, expected).zip(be_===, containTheSameElementsAs(_))
    }

    "retains mask stage when any path has type != top" >>* {
      val mask: ScalarStage = ScalarStage.Mask(Map(
        CPath.parse(".width") -> Set(ColumnType.Number),
        CPath.parse(".height") -> ColumnType.Top))

      val stages = ScalarStages(IdStatus.IncludeId, List(mask))

      val expected = List(
        Json("width" := 1.1, "height" := 2.2),
        Json("width" := 3.3, "height" := 4.4),
        Json("width" := 5.5, "height" := 6.6),
        Json("width" := 7.7, "height" := 8.8))

      for {
        _ <- loadWidgets(maskWidgets)
        r <- resultsOf[Json](widgetsRead(stages))
      } yield r must (stages, expected).zip(be_===, containTheSameElementsAs(_))
    }

    "retains mask stage when path has incompatible type" >>* {
      val mask: ScalarStage = ScalarStage.Mask(Map(
        CPath.parse(".width") -> Set(ColumnType.String),
        CPath.parse(".height") -> ColumnType.Top))

      val stages = ScalarStages(IdStatus.IncludeId, List(mask))

      val expected = List(
        Json("width" := 1.1, "height" := 2.2),
        Json("width" := 3.3, "height" := 4.4),
        Json("width" := 5.5, "height" := 6.6),
        Json("width" := 7.7, "height" := 8.8))

      for {
        _ <- loadWidgets(maskWidgets)
        r <- resultsOf[Json](widgetsRead(stages))
      } yield r must (stages, expected).zip(be_===, containTheSameElementsAs(_))
    }

    "retains deeper structure when initial field is masked" >>* {
      val setup =
        xa.trans.apply(for {
          _ <- sql"""DROP TABLE IF EXISTS "pgsrcSchemaA"."nested"""".update.run
          _ <- sql"""CREATE TABLE "pgsrcSchemaA"."nested" (x VARCHAR, y jsonb, z INT)""".update.run
          _ <- sql"""INSERT INTO "pgsrcSchemaA"."nested" (x, y, z) VALUES ('A', '{"foo": 1, "bar": [2, 3, 4]}', 42), ('B', '{"foo": 23, "baz": {"quux": 7, "bar": ["x", 17]}}', 91)""".update.run
        } yield ())

      val expected = List(
        Json("z" := 42, "y" := Json("foo" := 1, "bar" := List(2, 3, 4))),
        Json("z" := 91, "y" := Json("foo" := 23, "baz" := Json("quux" := 7, "bar" := List(jString("x"), jNumber(17))))))

      val mask: ScalarStage = ScalarStage.Mask(Map(
        CPath.parse(".z") -> Set(ColumnType.Number),
        CPath.parse(".y.bar") -> ColumnType.Top))

      val stages = ScalarStages(IdStatus.ExcludeId, List(mask))

      val read = InterpretedRead(
        ResourcePath.root() / ResourceName("pgsrcSchemaA") / ResourceName("nested"),
        stages)

      (setup >> resultsOf[Json](read))
        .map(_ must (stages, expected).zip(be_===, containTheSameElementsAs(_)))
    }
  }

  ////

  private final case class Widget(serial: String, width: Double, height: Double)

  private object Widget {
    implicit val widgetCodecJson: CodecJson[Widget] =
      casecodec3(Widget.apply, Widget.unapply)("serial", "width", "height")
  }

  private def loadWidgets(widgets: List[Widget]): IO[Unit] =
    xa.trans.apply(for {
      _ <- sql"""DROP TABLE IF EXISTS "pgsrcSchemaA"."widgets"""".update.run
      _ <- sql"""CREATE TABLE "pgsrcSchemaA"."widgets" (serial VARCHAR, width DECIMAL, height DECIMAL)""".update.run
      load = """INSERT INTO "pgsrcSchemaA"."widgets" (serial, width, height) VALUES (?, ?, ?)"""
      _ <- Update[Widget](load).updateMany(widgets)
    } yield ())

  private def widgetsRead(stages: ScalarStages = ScalarStages.Id)
      : InterpretedRead[ResourcePath] =
    InterpretedRead(
      ResourcePath.root() / ResourceName("pgsrcSchemaA") / ResourceName("widgets"),
      stages)

  private def resultsOf[A: DecodeJson](ir: InterpretedRead[ResourcePath])
      : IO[(ScalarStages, List[A])] =
    pgds.evaluate(ir) use {
      case QueryResult.Typed(_, s, stages) =>
        s.data.chunks.parseJsonStream[Json]
          .map(_.as[A].toOption)
          .unNone
          .compile.toList
          .tupleLeft(stages)

      case qr =>
        IO.raiseError(new RuntimeException(s"Expected QueryResult.Typed, received: $qr"))
    }
}
