import ProjectSettings.ProjectFrom

ThisBuild / scalaVersion        := "3.3.0"
ThisBuild / organization        := "it.agilelab"
ThisBuild / organizationName    := "AgileLab S.r.L."
ThisBuild / libraryDependencies := Dependencies.Jars.`server`
ThisBuild / dependencyOverrides ++= Dependencies.Jars.overrides
ThisBuild / version             := ComputeVersion.version
ThisBuild / packagePrefix := name.value.replaceFirst("uservice-", "uservice.").replaceAll("-", "")
val packagePrefix = settingKey[String]("The package prefix derived from the uservice name")

val specFile = file("src/main/resources/interface-specification.yml")

//lazy val client = (project in file("client")).settings(
//  name := "{{cookiecutter.app_name}}-client",
//  Compile / guardrailTasks += ScalaClient(specFile, pkg=s"it.agilelab.${packagePrefix.value}.client", framework="http4s"),
//  libraryDependencies := Dependencies.Jars.client,
//)

lazy val root = (project in file(".")).settings(
  name                        := "witboost.ontology.manager",
  Compile / guardrailTasks += ScalaServer(specFile, pkg=s"it.agilelab.${packagePrefix.value}", framework="http4s"),
  Test / parallelExecution    := false,
  dockerBuildOptions ++= Seq("--network=host"),
  dockerBaseImage             := "adoptopenjdk:11-jdk-hotspot",
  dockerUpdateLatest          := true,
  daemonUser                  := "daemon",
  Docker / version            := s"${
    val buildVersion = (ThisBuild / version).value
    if (buildVersion == "latest") buildVersion else s"v$buildVersion"
  }".toLowerCase,
  Docker / packageName        :=
    s"registry.gitlab.com/agilefactory/witboost.mesh/provisioning/uservice-rest-guardarail-template",
  Docker / dockerExposedPorts := Seq(8080)
).enablePlugins(JavaAppPackaging).setupBuildInfo