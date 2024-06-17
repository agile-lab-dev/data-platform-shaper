package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import fly4s.*
import fly4s.data.*
import fly4s.implicits.*
import cats.effect.*
import it.agilelab.dataplatformshaper.domain.common.db.Repository

case class JdbcRepository[F[_]: Sync](session: JdbcSession)
    extends Repository[F]:
  private def loadDbConfig(
    databaseConfig: DatabaseConfig
  ): Resource[IO, Fly4s[IO]] =
    Fly4s.make[IO](
      url = databaseConfig.url,
      user = databaseConfig.user,
      password = databaseConfig.password,
      config = Fly4sConfig(
        table = databaseConfig.migrationsTable,
        locations = Locations(databaseConfig.migrationsLocations)
      )
    )
  end loadDbConfig

  def migrateDb(databaseConfig: DatabaseConfig): Resource[IO, MigrateResult] =
    val fly4s = loadDbConfig(databaseConfig)
    fly4s.evalMap { f =>
      f.migrate.flatMap { result =>
        if result.success then IO.pure(result)
        else IO.raiseError(new RuntimeException("Migration failed"))
      }
    }
  end migrateDb

end JdbcRepository
