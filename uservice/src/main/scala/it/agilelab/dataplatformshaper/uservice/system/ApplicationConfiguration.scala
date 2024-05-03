package it.agilelab.dataplatformshaper.uservice.system

import buildinfo.BuildInfo
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}

import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

object ApplicationConfiguration:

  val config: AtomicReference[Config] = AtomicReference(ConfigFactory.load())

  def reloadConfig(): String = config.synchronized {
    @tailrec
    def snip(): Unit =
      val oldConf = config.get
      val newConf =
        ConfigFactory.invalidateCaches()
        ConfigFactory.load()
      if (!config.compareAndSet(oldConf, newConf)) snip()
    end snip
    snip()
    config.get
      .getObject("dataplatform.shaper.uservice")
      .render(ConfigRenderOptions.defaults())
  }
  end reloadConfig

  def httpPort: Int = config.get.getInt(s"${BuildInfo.name}.http-port")

  def graphdbType: String =
    config.get.getString(s"${BuildInfo.name}.graphdb-type")

  def graphdbPort: Int = config.get.getInt(s"${BuildInfo.name}.graphdb-port")

  def graphdbHost: String =
    config.get.getString(s"${BuildInfo.name}.graphdb-host")

  def graphdbUser: String =
    config.get.getString(s"${BuildInfo.name}.graphdb-user")

  def graphdbPwd: String =
    config.get.getString(s"${BuildInfo.name}.graphdb-pwd")

  def graphdbRepositoryId: String =
    config.get.getString(s"${BuildInfo.name}.graphdb-repository-id")

  def graphdbRepositoryTls: Boolean =
    config.get.getBoolean(s"${BuildInfo.name}.graphdb-repository-tls")

  def graphdbCreateRepo: Boolean =
    config.get.getBoolean(s"${BuildInfo.name}.graphdb-create-repo")

end ApplicationConfiguration
