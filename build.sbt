import scala.collection.Seq

performMavenCentralSync in ThisBuild := false   // basically just ignores all the sonatype sync parts of things

publishAsOSSProject in ThisBuild := true

homepage in ThisBuild := Some(url("https://github.com/slamdata/quasar-datasource-postgres"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/slamdata/quasar-datasource-postgres"),
  "scm:git@github.com:slamdata/quasar-datasource-postgres.git"))

val DoobieVersion = "0.8.8"

lazy val root = project
  .in(file("."))
  .settings(noPublishSettings)
  .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(
    name := "quasar-datasource-postgres",

    quasarPluginName := "postgres",

    quasarPluginQuasarVersion := IO.read(file("./quasar-version")).trim,

    quasarPluginDatasourceFqcn := Some("quasar.plugin.postgres.datasource.PostgresDatasourceModule$"),

    /** Specify managed dependencies here instead of with `libraryDependencies`.
      * Do not include quasar libs, they will be included based on the value of
      * `quasarPluginQuasarVersion`.
      */
    quasarPluginDependencies ++= Seq(
      "org.slf4s"    %% "slf4s-api"       % "1.7.25",
      "org.tpolecat" %% "doobie-core"     % DoobieVersion,
      "org.tpolecat" %% "doobie-hikari"   % DoobieVersion,
      "org.tpolecat" %% "doobie-postgres" % DoobieVersion
    ),

    libraryDependencies ++= Seq(
      // TODO: Depend on this by default in sbt-quasar-plugin
      "com.slamdata" %% "quasar-foundation" % quasarPluginQuasarVersion.value % "test->test",
      "org.http4s" %% "jawn-fs2" % "1.0.0" % Test
      // Using this for now instead of argonaut-json: https://github.com/slamdata/quasar/blob/v160.0.0/foundation/src/main/scala/quasar/contrib/argonaut/JawnParser.scala#L25
      // "io.argonaut" %% "argonaut-jawn" % "6.2.3" % Test
    ))
  .enablePlugins(AutomateHeaderPlugin, QuasarPlugin)
