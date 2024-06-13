package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import cats.effect.Sync
import it.agilelab.dataplatformshaper.domain.common.db.Repository

case class JdbcRepository[F[_]: Sync](session: JdbcSession)
    extends Repository[F]:
  ???
end JdbcRepository
