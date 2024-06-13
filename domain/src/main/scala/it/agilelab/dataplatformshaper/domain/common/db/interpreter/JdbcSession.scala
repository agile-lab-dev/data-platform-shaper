package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import cats.effect.*
import cats.implicits.*
import scalikejdbc.ConnectionPool
import it.agilelab.dataplatformshaper.domain.common.db.{Connection, Session}

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

  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  def withTx[F[_]: Sync, T](func: Connection => T): F[T] =
    throw new UnsupportedOperationException(
      "withTx is not supported for JdbcSession"
    )
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
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = s"jdbc:$dbType://$host:$port/$repositoryId"
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
