package it.agilelab.dataplatformshaper.domain.common.db

import cats.effect.Sync
import cats.effect.*

trait Session:
  def getSession(
    dbType: String,
    host: String,
    port: Int,
    user: String,
    pwd: String,
    repositoryId: String,
    tls: Boolean
  ): Session

  def withTx[F[_]: Sync, T](func: Connection => T): F[T]

end Session
