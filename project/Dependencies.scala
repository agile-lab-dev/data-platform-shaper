import Versions.*
import sbt.*

object Dependencies {

  private[this] object rdf4j {
    lazy val namespace = "org.eclipse.rdf4j"
    lazy val client    = namespace % "rdf4j-client" % rdf4jVersion
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

  private[this] object atlassian {
    lazy val namespace = "com.atlassian.oai"
    lazy val openapiValidator = namespace % "swagger-request-validator-core" % openapiValidatorVersion
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

  private[this] object opentelemetry {
    lazy val namespace    = "io.opentelemetry"
    lazy val api          = namespace % "opentelemetry-api" % opentelemetryVersion
    lazy val annotations  = s"$namespace.instrumentation" % "opentelemetry-instrumentation-annotations" % opentelemetryAnnotationsVersion
    lazy val sdk          = namespace % "opentelemetry-sdk" % opentelemetryVersion
    lazy val exporterOtlp = namespace % "opentelemetry-exporter-otlp" % opentelemetryVersion
  }

  object Jars {

    lazy val overrides: Seq[ModuleID] = Seq(
     )

    lazy val domain: Seq[ModuleID] = Seq(
      rdf4j.client                 % Compile,
      cats.core                    % Compile,
      cats.effect                  % Compile,
      cats.loggingCore             % Compile,
      cats.loggingSlf4j            % Compile,
      circe.core                   % Compile,
      circe.generic                % Compile,
      circe.parser                 % Compile,
      circe.yaml                   % Compile,
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
      atlassian.openapiValidator   % Compile,
      typesafe.config              % Compile,
      opentelemetry.api            % Compile,
      opentelemetry.annotations    % Compile,
      opentelemetry.sdk            % Compile,
      opentelemetry.exporterOtlp   % Compile,
      testcontainers.core          % Test,
      cats.effectScalatest         % Test,
      scalatest.core               % Test,
    )

  }
}
