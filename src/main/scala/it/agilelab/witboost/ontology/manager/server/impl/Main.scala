package it.agilelab.witboost.ontology.manager.server.impl

import cats.effect.{ExitCode, IO, IOApp, Ref}
import it.agilelab.witboost.ontology.manager.domain.knowledgegraph.interpreter.Session
import it.agilelab.witboost.ontology.manager.domain.model.l0.EntityType
import it.agilelab.witboost.ontology.manager.system.ApplicationConfiguration.{graphdbHost, graphdbPort, graphdbRepositoryId, graphdbRepositoryTls}

@SuppressWarnings(Array("org.wartremover.warts.Nothing"))
object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    val session = Session.getSession(graphdbHost, graphdbPort, graphdbRepositoryId, graphdbRepositoryTls)
    val typeCacheRef: IO[Ref[IO, Map[String, EntityType]]] =
      Ref[IO].of(Map.empty[String, EntityType])
    val server = for {
      typeCache <- typeCacheRef
    } yield {
      Server.server[IO](session, typeCache)
    }
    server.flatMap(_.use(_ => IO.never).as(ExitCode.Success))
  end run

end Main
