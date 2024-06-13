package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import cats.effect.*
import cats.effect.kernel.Resource.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.db.{Connection, Session}
import org.eclipse.rdf4j.model.IRI
import org.eclipse.rdf4j.repository.RepositoryConnection
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import virtuoso.rdf4j.driver.VirtuosoRepository

final case class Rdf4jSession(connection: Rdf4jConnection) extends Session:

  override def getSession(
    dbType: String,
    host: String,
    port: Int,
    user: String,
    pwd: String,
    repositoryId: String,
    tls: Boolean
  ): Session =
    Rdf4jSession.getSession(dbType, host, port, user, pwd, repositoryId, tls)

  def withTx[F[_]: Sync, T](func: Connection => T): F[T] =
    val logger: Logger[F] = Slf4jLogger.getLogger[F]
    Resource
      .makeCase(Sync[F].delay {
        this.connection.connection.begin()
        this.connection
      }) {
        case (connection, ExitCase.Succeeded) =>
          Sync[F].delay(connection.connection.commit()) *> logger.trace(
            s"Transaction committed"
          )
        case (connection, ExitCase.Errored(ex: Throwable)) =>
          Sync[F].delay(connection.connection.rollback()) *> logger.trace(
            s"Transaction rollback with error ${ex.getLocalizedMessage}"
          )
        case (connection, ExitCase.Canceled) =>
          Sync[F].delay(connection.connection.rollback()) *> logger.trace(
            s"Transaction rollback"
          )
      }
      .use(session => Sync[F].delay(func(Rdf4jConnection(session.connection))))
  end withTx

end Rdf4jSession

object Rdf4jSession:
  def getSession(
    dbType: String,
    host: String,
    port: Int,
    user: String,
    pwd: String,
    repositoryId: String,
    tls: Boolean
  ): Rdf4jSession =
    dbType match
      case "graphdb" =>
        val rdf4jServer =
          if tls then s"https://$host:$port" else s"http://$host:$port"
        val manager = RemoteRepositoryManager(rdf4jServer)
        manager.init()
        val conn: RepositoryConnection =
          manager.getRepository(repositoryId).getConnection
        Rdf4jSession(Rdf4jConnection(conn))
      case "virtuoso" =>
        val virtuosoRepository =
          VirtuosoRepository(s"jdbc:virtuoso://$host:$port", user, pwd)
        val context: IRI =
          virtuosoRepository.getValueFactory.createIRI(s"db:$repositoryId")
        val conn: RepositoryConnection = virtuosoRepository.getConnection
        conn.clear(context)
        Rdf4jSession(Rdf4jConnection(conn))
    end match
  end getSession

  def apply[F[_]: Sync](
    dbType: String,
    host: String,
    port: Int,
    user: String,
    pwd: String,
    repositoryId: String,
    tls: Boolean
  ): Resource[F, Session] =
    given logger: Logger[F] = Slf4jLogger.getLogger[F]
    Resource
      .make(
        (for {
          res <- Sync[F].delay {
            getSession(dbType, host, port, user, pwd, repositoryId, tls)
          }
          _ <- logger.trace(s"Session acquired for the endpoint")
        } yield res).onError(* =>
          logger.error(s"Error in connecting to the db")
        )
      )(session =>
        Sync[F].delay {
          session.connection.connection.close()
          ()
        }
      )
  end apply
end Rdf4jSession
