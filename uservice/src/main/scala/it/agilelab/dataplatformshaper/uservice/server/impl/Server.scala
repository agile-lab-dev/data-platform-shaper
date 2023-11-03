package it.agilelab.dataplatformshaper.uservice.server.impl

import buildinfo.BuildInfo
import cats.effect.{Async, Ref, Resource}
import cats.implicits.*
import com.comcast.ip4s.Port
import fs2.io.net.Network
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.l0.EntityType
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TypeManagementServiceInterpreter,
  TraitManagementServiceInterpreter
}
import it.agilelab.dataplatformshaper.uservice.system.ApplicationConfiguration.httpPort
import it.agilelab.dataplatformshaper.uservice.Resource as GenResource
import it.agilelab.dataplatformshaper.uservice.api.intepreter.OntologyManagerHandler
import org.http4s.HttpRoutes
import org.http4s.dsl.Http4sDsl
import org.http4s.ember.server.EmberServerBuilder
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

  def server[F[_]: Async: Network](
      session: Session,
      typeCache: Ref[F, Map[String, EntityType]]
  ): Resource[F, Unit] =
    val dsl = Http4sDsl[F]
    import dsl.*

    given cache: Ref[F, Map[String, EntityType]] = typeCache

    val repository = Rdf4jKnowledgeGraph[F](session)
    val trms = new TraitManagementServiceInterpreter[F](repository)
    val tms = new TypeManagementServiceInterpreter[F](trms)
    val ims = new InstanceManagementServiceInterpreter[F](tms)

    val assetsRoutes = resourceServiceBuilder("/").toRoutes

    val interfaceFileRoute = HttpRoutes.of[F] {
      case GET -> Root / "interface-specification.yml" =>
        Ok(interfaceString)
    }

    val allRoutes =
      interfaceFileRoute <+>
        assetsRoutes <+>
        GenResource[F]().routes(new OntologyManagerHandler[F](tms, ims, trms))

    for _ <- EmberServerBuilder
        .default[F]
        .withHttpApp(
          Router(
            s"${BuildInfo.name}/${BuildInfo.interfaceVersion}/" -> allRoutes
          ).orNotFound
        )
        .withPort(Port.fromInt(httpPort).get)
        .build
    yield ()
  end server

end Server
