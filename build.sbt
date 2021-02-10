import scala.collection.Seq

ThisBuild / scalaVersion := "2.12.10"

ThisBuild / githubRepository := "quasar-datasource-postgres"

performMavenCentralSync in ThisBuild := false   // basically just ignores all the sonatype sync parts of things

publishAsOSSProject in ThisBuild := true

homepage in ThisBuild := Some(url("https://github.com/precog/quasar-datasource-postgres"))

scmInfo in ThisBuild := Some(ScmInfo(
  url("https://github.com/precog/quasar-datasource-postgres"),
  "scm:git@github.com:precog/quasar-datasource-postgres.git"))

ThisBuild / githubWorkflowBuildMatrixAdditions +=
  "postgres" -> List("9", "10", "11")

ThisBuild / githubWorkflowBuildPreamble +=
  WorkflowStep.Run(
    List("docker-compose up -d postgres${{ matrix.postgres }}"),
    name = Some("Start postgres ${{ matrix.postgres }} container"))

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

    quasarPluginQuasarVersion := managedVersions.value("precog-quasar"),

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
      "com.precog" %% "quasar-foundation" % quasarPluginQuasarVersion.value % "test->test",
      "org.http4s" %% "jawn-fs2" % "1.0.0-RC2" % Test,
      "io.argonaut" %% "argonaut-jawn" % "6.3.0-M2" % Test
    ))
  .enablePlugins(QuasarPlugin)
  .evictToLocal("QUASAR_PATH", "connector", true)
  .evictToLocal("QUASAR_PATH", "api", true)
