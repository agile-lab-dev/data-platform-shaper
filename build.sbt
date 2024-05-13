import ProjectSettings.ProjectFrom
import com.typesafe.sbt.packager.docker.{Cmd, ExecCmd}

import java.io.{BufferedWriter, PrintWriter}
import java.nio.file.Files
import scala.io.Codec.UTF8
import scala.io.Source

ThisBuild / scalaVersion             := "3.4.2"
ThisBuild / organization             := "it.agilelab"
ThisBuild / organizationName         := "AgileLab S.r.L."
ThisBuild / dependencyOverrides     ++= Dependencies.Jars.overrides
ThisBuild / version                  := ComputeVersion.version
ThisBuild / semanticdbEnabled        := true
ThisBuild / Test / parallelExecution := false


val serviceName       = "dataplatform.shaper.uservice"
val interfaceSpecFile = "uservice/src/main/resources/interface-specification.yml"

def clientInterfaceFile: File = {
  val source = Source
    .fromFile(interfaceSpecFile)(UTF8)
  val interfaceString = source.getLines()
    .map(_.replaceAll("__VERSION__", ProjectSettings.interfaceVersion))
    .map(
      _.replaceAll(
        "#__URL__",
        s"url: /$serviceName/${ProjectSettings.interfaceVersion}"
      )
    )
    .mkString("\n")
  source.close()
  val tmpFilePath = Files.createTempFile("openapi","")
  val bufferedPrintWriter = new BufferedWriter(new PrintWriter(tmpFilePath.toFile))
  bufferedPrintWriter.write(interfaceString)
  bufferedPrintWriter.flush()
  tmpFilePath.toFile
}

lazy val domain = (project in file("domain")).settings(
  name                     := "dataplatform.shaper.domain",
  libraryDependencies      := Dependencies.Jars.domain
)

lazy val userviceClientGenerated = (project in file("uservice-client-generated")).settings(
  name                                   := s"$serviceName-client-generated",
  libraryDependencies                    := Dependencies.Jars.uservice,
  Compile / scalacOptions                := Seq(),
  Compile / guardrailTasks               += ScalaClient(clientInterfaceFile, pkg=s"it.agilelab.dataplatformshaper.uservice", framework="http4s"),
  Compile / packageDoc / publishArtifact := false
)

lazy val userviceGenerated = (project in file("uservice-generated")).settings(
  name                                   := s"$serviceName-generated",
  libraryDependencies                    := Dependencies.Jars.uservice,
  Compile / scalacOptions                := Seq(),
  Compile / guardrailTasks               += ScalaServer(file(interfaceSpecFile), pkg=s"it.agilelab.dataplatformshaper.uservice", framework="http4s"),
  Compile / packageDoc / publishArtifact := false
)

lazy val uservice = (project in file("uservice")).settings(
  name                                    := serviceName,
  libraryDependencies                     := Dependencies.Jars.uservice,
  dockerBaseImage                         := "eclipse-temurin:21",
  dockerUpdateLatest                      := true,
  daemonUser                              := "daemon",
  Docker / version                        := s"${
    val buildVersion = (ThisBuild / version).value
    if (buildVersion == "latest") buildVersion else s"v$buildVersion"
  }".toLowerCase,
  Docker / dockerCommands                ++= Seq(
    // setting the run script executable
    Cmd("USER", "root"),
    ExecCmd("RUN", "curl", "-L", "https://github.com/cue-lang/cue/releases/download/v0.7.0/cue_v0.7.0_linux_amd64.tar.gz", "-o", "cue.tar.gz"),
    ExecCmd("RUN", "mkdir", "/usr/local/cue"),
    ExecCmd("RUN", "tar", "-C", "/usr/local/cue", "-xzf", "cue.tar.gz"),
    ExecCmd("RUN", "rm", "cue.tar.gz"),
    ExecCmd("RUN", "ln", "-sf", "/usr/local/cue/cue", "/usr/local/bin")
  ),
  Docker / dockerExposedPorts             := Seq(8093)
).dependsOn(domain, userviceGenerated, userviceClientGenerated % "test->compile").enablePlugins(JavaAppPackaging).setupBuildInfo

lazy val docs = (project in file("docs")).
  enablePlugins(SitePreviewPlugin, ParadoxPlugin).
  settings(
    paradoxTheme := Some(builtinParadoxTheme("generic")),
    Compile / paradoxProperties ++= Map(
     "extref.site.base_url" -> "./%s"
    )
  )