package it.agilelab.dataplatformshaper.uservice.server.impl

import buildinfo.BuildInfo
import cats.effect.{Async, Resource, Sync}
import cats.implicits.*
import com.comcast.ip4s.{Port, ipv4}
import fs2.io.net.Network
import io.chrisdavenport.mules.Cache
import it.agilelab.dataplatformshaper.domain.common.db.{Repository, Session}
import it.agilelab.dataplatformshaper.domain.common.db.interpreter.{
  JdbcRepository,
  JdbcSession,
  Rdf4jRepository,
  Rdf4jSession
}
import it.agilelab.dataplatformshaper.domain.service.interpreter.rdf4j.{
  InstanceManagementServiceInterpreter as rdf4jInstanceManagementServiceInterpreter,
  MappingManagementServiceInterpreter as rdf4jMappingManagementServiceInterpreter,
  TraitManagementServiceInterpreter as rdf4jTraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter as rdf4jTypeManagementServiceInterpreter
}
import it.agilelab.dataplatformshaper.domain.model.EntityType
import it.agilelab.dataplatformshaper.domain.service.{
  InstanceManagementService,
  MappingManagementService,
  TraitManagementService,
  TypeManagementService
}
import it.agilelab.dataplatformshaper.uservice.Resource as GenResource
import it.agilelab.dataplatformshaper.uservice.api.intepreter.OntologyManagerHandler
import it.agilelab.dataplatformshaper.uservice.system.ApplicationConfiguration.httpPort
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server.*
import org.http4s.implicits.*
import org.http4s.server.Router
import org.http4s.server.staticcontent.resourceServiceBuilder

import scala.io.Codec.UTF8
import scala.io.Source

object Server:

  val interfaceString: String = Source
    .fromResource("interface-specification.yml")(UTF8)
    .getLines()
    .map(_.replaceAll("__VERSION__", BuildInfo.interfaceVersion))
    .map(
      _.replaceAll(
        "#__URL__",
        s"url: /${BuildInfo.name}/${BuildInfo.interfaceVersion}"
      )
    )
    .mkString("\n")

  private def getManagementServices[F[_]: Sync](
    repository: Repository[F]
  )(using cache: Cache[F, String, EntityType]): (
    TraitManagementService[F],
    TypeManagementService[F],
    InstanceManagementService[F],
    MappingManagementService[F]
  ) =
    repository match
      case jdbcRepository: JdbcRepository[F] => // TODO: Change with real ones
        val traitManagementService =
          rdf4jTraitManagementServiceInterpreter[F](repository)
        val typeManagementService =
          rdf4jTypeManagementServiceInterpreter[F](traitManagementService)
        val instanceManagementService =
          rdf4jInstanceManagementServiceInterpreter[F](typeManagementService)
        val mappingManagementService =
          rdf4jMappingManagementServiceInterpreter[F](
            typeManagementService,
            instanceManagementService
          )
        (
          traitManagementService,
          typeManagementService,
          instanceManagementService,
          mappingManagementService
        )
      case rdf4jRepository: Rdf4jRepository[F] =>
        val traitManagementService =
          rdf4jTraitManagementServiceInterpreter[F](repository)
        val typeManagementService =
          rdf4jTypeManagementServiceInterpreter[F](traitManagementService)
        val instanceManagementService =
          rdf4jInstanceManagementServiceInterpreter[F](typeManagementService)
        val mappingManagementService =
          rdf4jMappingManagementServiceInterpreter[F](
            typeManagementService,
            instanceManagementService
          )
        (
          traitManagementService,
          typeManagementService,
          instanceManagementService,
          mappingManagementService
        )
  end getManagementServices

  def server[F[_]: Async: Network](
    session: Session,
    typeCache: Cache[F, String, EntityType]
  ): Resource[F, Unit] =
    val dsl = Http4sDsl[F]
    import dsl.*

    given cache: Cache[F, String, EntityType] = typeCache

    val repository: Repository[F] = session match
      case session: JdbcSession  => JdbcRepository[F](session)
      case session: Rdf4jSession => Rdf4jRepository[F](session)
    val (trms, tms, ims, mms) = getManagementServices(repository)

    val assetsRoutes = resourceServiceBuilder("/").toRoutes

    val interfaceFileRoute = HttpRoutes.of[F] {
      case GET -> Root / "interface-specification.yml" =>
        Ok(interfaceString)
    }

    val allRoutes =
      interfaceFileRoute <+>
        assetsRoutes <+>
        GenResource[F]().routes(OntologyManagerHandler[F](tms, ims, trms, mms))

    for _ <- EmberServerBuilder
        .default[F]
        .withHttpApp(
          Router(
            s"${BuildInfo.name}/${BuildInfo.interfaceVersion}/" -> allRoutes
          ).orNotFound
        )
        .withPort(Port.fromInt(httpPort).get)
        .withHost(ipv4"0.0.0.0")
        .build
    yield ()
  end server

end Server
