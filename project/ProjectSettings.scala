import sbt.Project
import sbtbuildinfo.BuildInfoKeys.buildInfoOptions
import sbtbuildinfo.BuildInfoPlugin.autoImport.{buildInfoKeys, BuildInfoKey}
import sbtbuildinfo.{BuildInfoOption, BuildInfoPlugin}

import scala.sys.process._
import scala.util.Try

/** Allows customizations of build.sbt syntax. */
object ProjectSettings {

  private val currentBranch: Option[String] = Try(Process(s"git branch --show-current").lineStream_!.head).toOption

  private val commitSha: Option[String] = Try(Process(s"git rev-parse --short HEAD").lineStream_!.head).toOption

  private val interfaceVersion: String =
    ComputeVersion.version.split("\\.") match { case Array(major, minor, _*) => s"$major.$minor" }

  // lifts some useful data in BuildInfo instance
  val buildInfoExtra: Seq[BuildInfoKey] = Seq[BuildInfoKey](
    "ciBuildNumber"    -> sys.env.get("BUILD_NUMBER"),
    "commitSha"        -> commitSha,
    "currentBranch"    -> currentBranch,
    "interfaceVersion" -> interfaceVersion
  )

  /** Extention methods for sbt Project instances.
   *  @param project
   */
  implicit class ProjectFrom(project: Project) {

    def setupBuildInfo: Project = project.enablePlugins(BuildInfoPlugin).settings(buildInfoKeys ++= buildInfoExtra)
      .settings(buildInfoOptions += BuildInfoOption.BuildTime).settings(buildInfoOptions += BuildInfoOption.ToJson)
  }
}
