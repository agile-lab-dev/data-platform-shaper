import Versions.*
import sbt.*

object Dependencies {

  private[this] object rdf4j {
    lazy val namespace    = "org.eclipse.rdf4j"
    lazy val client       = namespace % "rdf4j-client" % rdf4jVersion
    lazy val queryAlgebra = namespace % "rdf4j-queryalgebra-evaluation" % rdf4jVersion
  }

  private[this] object virtuoso {
    lazy val namespace   = "com.openlinksw"
    lazy val rdf4jDriver = namespace % "virt_rdf4j_v4_0" % virtuosoRDF4JVersion
    lazy val jdbcDriver  = namespace % "virtjdbc4_2" % virtuosoJDBCVersion
  }

  private[this] object dataTools {
    lazy val namespace = "io.github.data-tools"
    lazy val dataTypesCore = namespace %% "big-data-types-core" % dataToolsVersion
  }

  private[this] object http4s {
    lazy val namespace   = "org.http4s"
    lazy val emberServer = namespace %% "http4s-ember-server" % http4sVersion
    lazy val emberClient = namespace %% "http4s-ember-client" % http4sVersion
    lazy val circe       = namespace %% "http4s-circe" % http4sVersion
    lazy val dsl         = namespace %% "http4s-dsl" % http4sVersion
  }

  private[this] object cats {
    lazy val namespace = "org.typelevel"
    lazy val core = namespace %% "cats-core" % catsVersion
    lazy val effect = namespace %% "cats-effect" % catsEffectVersion withSources() withJavadoc()
    lazy val effectScalatest = namespace %% "cats-effect-testing-scalatest" % catEffectTestingVersion
    lazy val loggingCore = namespace %% "log4cats-core" % catsEffectLoggingVersion
    lazy val loggingSlf4j = namespace %% "log4cats-slf4j" % catsEffectLoggingVersion
  }

  private[this] object typesafe {
    lazy val namespace = "com.typesafe"
    lazy val config = namespace % "config" % typesafeConfig
  }

  private[this] object circe {
    lazy val namespace = "io.circe"
    lazy val core      = namespace %% "circe-core"    % circeVersion
    lazy val generic   = namespace %% "circe-generic" % circeVersion
    lazy val parser    = namespace %% "circe-parser"  % circeVersion
    lazy val yaml      = namespace %% "circe-yaml"    % circeYamlVersion
  }

  private[this] object calcite {
    lazy val namespace = "org.apache.calcite"
    lazy val babel     = namespace % "calcite-core" % calciteVersion
  }

  private[this] object logging {
    val namespace = "com.typesafe.scala-logging"
    val scala     = namespace %% "scala-logging" % scalaLogging
  }

  private[this] object logback {
    lazy val namespace = "ch.qos.logback"
    lazy val classic   = namespace % "logback-classic" % logbackVersion
  }
  
  private[this] object scalatest {
    lazy val namespace = "org.scalatest"
    lazy val core      = namespace %% "scalatest" % scalatestVersion
  }

  private[this] object testcontainers {
    lazy val namespace = "org.testcontainers"
    lazy val core      = namespace % "testcontainers" % testContainersVersion
  }

  object Jars {

    lazy val overrides: Seq[ModuleID] = Seq(
      "org.slf4j" % "jcl-over-slf4j" % "2.0.9",
      "org.slf4j" % "slf4j-api" % "2.0.9"
    )

    lazy val domain: Seq[ModuleID] = Seq(
      rdf4j.client                 % Compile,
      rdf4j.queryAlgebra           % Compile,
      virtuoso.rdf4jDriver         % Compile,
      virtuoso.jdbcDriver          % Compile,
      cats.core                    % Compile,
      cats.effect                  % Compile,
      cats.loggingCore             % Compile,
      cats.loggingSlf4j            % Compile,
      circe.core                   % Compile,
      circe.generic                % Compile,
      circe.parser                 % Compile,
      circe.yaml                   % Compile,
      calcite.babel                % Compile,
      logging.scala                % Compile,
      logback.classic              % Compile,
      dataTools.dataTypesCore      % Compile,
      http4s.emberClient           % Test,
      testcontainers.core          % Test,
      cats.effectScalatest         % Test,
      scalatest.core               % Test
    )

    lazy val uservice: Seq[ModuleID] = Seq(
      http4s.emberServer           % Compile,
      http4s.emberClient           % Compile,
      http4s.dsl                   % Compile,
      http4s.circe                 % Compile,
      typesafe.config              % Compile,
      testcontainers.core          % Test,
      cats.effectScalatest         % Test,
      scalatest.core               % Test,
    )

  }
}
