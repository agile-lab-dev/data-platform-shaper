package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import cats.effect.*
import cats.effect.kernel.Resource.ExitCase
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.db.{Connection, Session}
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalikejdbc.ConnectionPool

final case class JdbcSession(connection: JdbcConnection) extends Session:

  override def getSession(
    dbType: String,
    host: String,
    port: Int,
    user: String,
    pwd: String,
    repositoryId: String,
    tls: Boolean
  ): Session =
    JdbcSession.getSession(dbType, host, port, user, pwd, repositoryId, tls)

  def withTx[F[_]: Sync, T](func: Connection => T): F[T] =
    val logger = Slf4jLogger.getLogger[F]
    Resource
      .makeCase(Sync[F].delay {
        val conn = connection.connection
        conn.setAutoCommit(false)
        conn
      }) {
        case (conn, ExitCase.Succeeded) =>
          Sync[F].delay(conn.commit()) *> logger.trace("Transaction committed")
        case (conn, ExitCase.Errored(ex)) =>
          Sync[F].delay(conn.rollback()) *> logger.trace(
            s"Transaction rollback with error ${ex.getLocalizedMessage}"
          )
        case (conn, ExitCase.Canceled) =>
          Sync[F].delay(conn.rollback()) *> logger.trace("Transaction rollback")
      }
      .use(conn => Sync[F].delay(func(JdbcConnection(conn))))
  end withTx

end JdbcSession

object JdbcSession:
  def getSession(
    dbType: String,
    host: String,
    port: Int,
    user: String,
    pwd: String,
    repositoryId: String,
    tls: Boolean
  ): JdbcSession =
    val driver = "org.postgresql.Driver"
    val url = s"jdbc:postgresql://$host:$port/$repositoryId"
    Class.forName(driver)
    ConnectionPool.singleton(url, user, pwd)
    JdbcSession(JdbcConnection(ConnectionPool.borrow()))
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
    Resource.make {
      Sync[F].delay {
        getSession(dbType, host, port, user, pwd, repositoryId, tls)
      }
    } { session =>
      Sync[F].delay {
        session.connection.connection.close()
      }
    }
  end apply
end JdbcSession
