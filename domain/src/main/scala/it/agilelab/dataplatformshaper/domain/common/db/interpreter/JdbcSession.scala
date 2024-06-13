package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import cats.effect.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.db.{Connection, Session}

final case class JdbcSession(connection: JdbcConnection)
    extends Session:

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
    ???
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
    ???
  end apply
end JdbcSession
