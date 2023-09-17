import ProjectSettings.ProjectFrom

ThisBuild / scalaVersion         := "3.3.1"
ThisBuild / organization         := "it.agilelab"
ThisBuild / organizationName     := "AgileLab S.r.L."
ThisBuild / dependencyOverrides ++= Dependencies.Jars.overrides
ThisBuild / version              := ComputeVersion.version
ThisBuild / semanticdbEnabled    := true

val specFile = file("uservice/src/main/resources/interface-specification.yml")

lazy val domain = (project in file("domain")).settings(
  name                                     := "witboost.ontology.manager.domain",
  libraryDependencies                      := Dependencies.Jars.domain,
  Test / parallelExecution               := false
)

lazy val userviceGenerated = (project in file("uservice-generated")).settings(
  name                      := "witboost.ontology.manager.uservice-generated",
  libraryDependencies       := Dependencies.Jars.uservice,
  Compile / scalacOptions   := Seq(),
  Compile / guardrailTasks  += ScalaServer(specFile, pkg=s"it.agilelab.witboost.ontology.manager.uservice", framework="http4s"),
)

lazy val uservice = (project in file("uservice")).settings(
  name                                    := "witboost.ontology.manager.uservice",
  libraryDependencies                     := Dependencies.Jars.uservice,
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
