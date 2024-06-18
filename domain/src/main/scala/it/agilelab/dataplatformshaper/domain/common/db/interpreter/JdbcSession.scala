package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import cats.effect.*
import cats.effect.kernel.Resource.ExitCase
import cats.implicits.*
import scalikejdbc.ConnectionPool
import it.agilelab.dataplatformshaper.domain.common.db.{Connection, Session}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

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
    val logger: Logger[F] = Slf4jLogger.getLogger[F]
    Resource
      .makeCase(Sync[F].delay {
        val conn = connection.connection
        conn.setAutoCommit(false)
        conn
      }) {
        case (conn, ExitCase.Succeeded) =>
          (for
            _ <- Sync[F].delay(conn.commit())
            _ <- Sync[F].delay(conn.close())
            _ <- logger.trace("Transaction committed")
          yield ()).handleErrorWith { ex =>
            logger.error(ex)("Error during commit or close")
          }
        case (conn, ExitCase.Errored(ex: Throwable)) =>
          (for
            _ <- Sync[F].delay(conn.rollback())
            _ <- Sync[F].delay(conn.close())
            _ <- logger.trace(
              s"Transaction rollback with error ${ex.getLocalizedMessage}"
            )
          yield ()).handleErrorWith { ex2 =>
            logger.error(ex2)("Error during rollback or close")
          }
        case (conn, ExitCase.Canceled) =>
          (for
            _ <- Sync[F].delay(conn.rollback())
            _ <- Sync[F].delay(conn.close())
            _ <- logger.trace("Transaction rollback")
          yield ()).handleErrorWith { ex =>
            logger.error(ex)("Error during rollback or close")
          }
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
