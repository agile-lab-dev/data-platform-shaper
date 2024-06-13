package it.agilelab.dataplatformshaper.uservice.server.impl
import cats.effect.std.Random
import cats.effect.{ExitCode, IO, IOApp}
import io.chrisdavenport.mules.caffeine.CaffeineCache
import io.chrisdavenport.mules.{Cache, TimeSpec}
import it.agilelab.dataplatformshaper.domain.common.db.{Repository, Session}
import it.agilelab.dataplatformshaper.domain.common.db.interpreter.{
  JdbcRepository,
  JdbcSession,
  Rdf4jRepository,
  Rdf4jSession
}
import it.agilelab.dataplatformshaper.domain.model.EntityType
import it.agilelab.dataplatformshaper.uservice.system.ApplicationConfiguration.*
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.multipart.{Multipart, Multiparts, Part}
import org.http4s.{EntityEncoder, Method, Request, Uri}

import scala.concurrent.duration.*

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
      session = Rdf4jSession.getSession(
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

  private def loadInitialOntologies(session: Session): IO[Unit] = {
    val repository: Repository[IO] = session match
      case session: JdbcSession  => JdbcRepository[IO](session)
      case session: Rdf4jSession => Rdf4jRepository[IO](session)
    repository match
      case rdf4jRepository: Rdf4jRepository[IO] =>
        rdf4jRepository.loadBaseOntologies()
      case jdbcRepository: JdbcRepository[IO] =>
        IO.unit // TODO actual implementation
  }

end Main
