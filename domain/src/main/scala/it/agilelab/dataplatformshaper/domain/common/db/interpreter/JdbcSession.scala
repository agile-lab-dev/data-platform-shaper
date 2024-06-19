package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import cats.effect.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.db.{Connection, Session}
import scalikejdbc.{ConnectionPool, DB, DBSession}

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
    Sync[F].delay {
      DB localTx { implicit session =>
        func(JdbcConnection(session.connection))
      }
    }
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
