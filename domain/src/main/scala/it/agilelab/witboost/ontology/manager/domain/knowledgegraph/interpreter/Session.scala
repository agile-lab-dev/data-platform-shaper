package it.agilelab.witboost.ontology.manager.domain.knowledgegraph.interpreter

import cats.effect.*
import cats.effect.kernel.Resource.*
import cats.implicits.*
import org.eclipse.rdf4j.repository.RepositoryConnection
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

final case class Session(connection: RepositoryConnection):

  def withTx[F[_]: Sync, T](func: RepositoryConnection => T): F[T] =
    val logger: Logger[F] = Slf4jLogger.getLogger[F]
    Resource
      .makeCase(Sync[F].delay {
        this.connection.begin()
        this.connection
      }) {
        case (connection, ExitCase.Succeeded) =>
          Sync[F].delay(connection.commit()) *> logger.trace(
            s"Transaction committed"
          )
        case (connection, ExitCase.Errored(ex: Throwable)) =>
          Sync[F].delay(connection.rollback()) *> logger.trace(
            s"Transaction rollback with error ${ex.getLocalizedMessage}"
          )
        case (connection, ExitCase.Canceled) =>
          Sync[F].delay(connection.rollback()) *> logger.trace(
            s"Transaction rollback"
          )
      }
      .use(session => Sync[F].delay(func(session)))
  end withTx

end Session

object Session:

  def getSession(
      host: String,
      port: Int,
      repositoryId: String,
      tls: Boolean
  ): Session =
    val rdf4jServer =
      if tls then s"https://$host:$port" else s"http://$host:$port"
    val manager = new RemoteRepositoryManager(rdf4jServer)
    manager.init()
    Session(manager.getRepository(repositoryId).getConnection)
  end getSession

  def apply[F[_]: Sync](
      host: String,
      port: Int,
      repositoryId: String,
      tls: Boolean
  ): Resource[F, Session] =
    given logger: Logger[F] = Slf4jLogger.getLogger[F]
    val rdf4jServer =
      if tls then s"https://$host:$port" else s"http://$host:$port"
    Resource
      .make((for {
        res <- Sync[F].delay {
          val manager = new RemoteRepositoryManager(rdf4jServer)
          manager.init()
          Session(manager.getRepository(repositoryId).getConnection)
        }
        _ <- logger.trace(s"Session acquired for endpoint $rdf4jServer")
      } yield res).onError(* => logger.error(s"Error in $rdf4jServer")))(
        session =>
          Sync[F].delay {
            session.connection.close()
            ()
          }
      )
  end apply
end Session
