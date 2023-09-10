import ProjectSettings.ProjectFrom
import sbt.Test

ThisBuild / scalaVersion         := "3.3.0"
ThisBuild / organization         := "it.agilelab"
ThisBuild / organizationName     := "AgileLab S.r.L."
ThisBuild / libraryDependencies  := Dependencies.Jars.`server`
ThisBuild / dependencyOverrides ++= Dependencies.Jars.overrides
ThisBuild / version              := ComputeVersion.version

val specFile = file("uservice/src/main/resources/interface-specification.yml")

lazy val domain = (project in file("domain")).settings(
  name                                   := "witboost.ontology.manager.domain",
  wartremoverExcluded                    += sourceManaged.value,
  Compile / scalacOptions               ++= Seq(
    "-Wunused:imports",
    "-Wvalue-discard",
    "-encoding", "utf8",
    "-feature"),
  Compile / compile / wartremoverErrors ++= Warts.all,
  Test / parallelExecution                := false,
)

lazy val userviceGenerated = (project in file("uservice-generated")).settings(
  name                      := "witboost.ontology.manager.uservice-generated",
  Compile / scalacOptions  ++= Seq(),
  Compile / guardrailTasks  += ScalaServer(specFile, pkg=s"it.agilelab.witboost.ontology.manager.uservice", framework="http4s"),
)

lazy val uservice = (project in file("uservice")).settings(
  name                                    := "witboost.ontology.manager.uservice",
  wartremoverExcluded                     += sourceManaged.value,
  Compile / scalacOptions                ++= Seq(
    "-Wunused:imports",
    "-Wvalue-discard",
    "-encoding", "utf8",
    "-feature"),
  Compile / compile / wartremoverErrors  ++= Warts.all,
  Test / parallelExecution                := false,
  dockerBuildOptions                     ++= Seq("--network=host"),
  dockerBaseImage                         := "adoptopenjdk:11-jdk-hotspot",
  dockerUpdateLatest                      := true,
  daemonUser                              := "daemon",
  Docker / version                        := s"${
    val buildVersion = (ThisBuild / version).value
    if (buildVersion == "latest") buildVersion else s"v$buildVersion"
  }".toLowerCase,
  Docker / dockerExposedPorts             := Seq(8080)
).dependsOn(domain, userviceGenerated).enablePlugins(JavaAppPackaging).setupBuildInfo