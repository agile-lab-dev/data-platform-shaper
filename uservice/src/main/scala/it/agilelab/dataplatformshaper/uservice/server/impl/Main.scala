package it.agilelab.dataplatformshaper.uservice.server.impl
import cats.effect.std.Random
import cats.effect.{ExitCode, IO, IOApp}
import io.chrisdavenport.mules.caffeine.CaffeineCache
import io.chrisdavenport.mules.{Cache, TimeSpec}
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.NS
import it.agilelab.dataplatformshaper.domain.model.NS.{L0, L1, ns}
import it.agilelab.dataplatformshaper.domain.model.l0.EntityType
import it.agilelab.dataplatformshaper.uservice.system.ApplicationConfiguration.*
import org.eclipse.rdf4j.model.util.Values.iri
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.http4s.{EntityEncoder, Method, Request, Uri}

import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    val typeCacheIO: IO[Cache[IO, String, EntityType]] =
      CaffeineCache.build[IO, String, EntityType](
        Some(TimeSpec.unsafeFromDuration(1800.second)),
        None,
        None
      ) // TODO magic number
    val server = for {
      _ <- createRepository
      session = Session.getSession(
        graphdbType,
        graphdbHost,
        graphdbPort,
        graphdbUser,
        graphdbPwd,
        graphdbRepositoryId,
        graphdbRepositoryTls
      )
      _ <- loadInitialOntologies(session)
      typeCache <- typeCacheIO
    } yield {
      Server.server[IO](session, typeCache)
    }

    server.flatMap(_.use(_ => IO.never).as(ExitCode.Success))
  end run

  private def createRepository: IO[Unit] =
    if graphdbCreateRepo
    then
      val multiparts = Random
        .scalaUtilRandom[IO]
        .map(Multiparts.fromRandom[IO])
        .syncStep(Int.MaxValue)
        .unsafeRunSync()
        .toOption
        .get

      EmberClientBuilder
        .default[IO]
        .build
        .use { client =>
          multiparts
            .multipart(
              Vector(
                Part
                  .fileData[IO](
                    "config",
                    Thread
                      .currentThread()
                      .getContextClassLoader
                      .getResource("repo-config.ttl")
                  )
              )
            )
            .flatTap { multipart =>
              val entity = EntityEncoder[IO, Multipart[IO]].toEntity(multipart)
              val body = entity.body
              val request = Request(
                method = Method.POST,
                uri = Uri
                  .unsafeFromString(
                    s"http://$graphdbHost:$graphdbPort/rest/repositories"
                  ),
                body = body,
                headers = multipart.headers
              )
              client.expect[String](request)
            }
        } map (_ => ())
    else IO.pure(())
    end if
  end createRepository

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.null"
    )
  )
  private def loadInitialOntologies(session: Session): IO[Unit] = {
    val repository = Rdf4jKnowledgeGraph[IO](session)

    val model1 = Rio.parse(
      Thread.currentThread.getContextClassLoader
        .getResourceAsStream("dp-ontology-l0.ttl"),
      ns.getName,
      RDFFormat.TURTLE,
      L0
    )

    val model2 = Rio.parse(
      Thread.currentThread.getContextClassLoader
        .getResourceAsStream("dp-ontology-l1.ttl"),
      ns.getName,
      RDFFormat.TURTLE,
      L1
    )
    val statements1 = model1.getStatements(null, null, null, iri(ns, "L0"))
    val statements2 = model2.getStatements(null, null, null, iri(ns, "L1"))

    val loadInitialOntologies = repository.removeAndInsertStatements(
      statements1.asScala.toList ++ statements2.asScala.toList
    )
    loadInitialOntologies
  }

end Main
