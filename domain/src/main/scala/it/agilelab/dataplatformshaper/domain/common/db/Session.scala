package it.agilelab.dataplatformshaper.domain.common.db

import cats.effect.Sync
import cats.effect.*
import org.eclipse.rdf4j.repository.RepositoryConnection

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

  def withTx[F[_]: Sync, T](func: RepositoryConnection => T): F[T]

end Session
