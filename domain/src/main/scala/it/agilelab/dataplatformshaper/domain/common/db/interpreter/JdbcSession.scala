package it.agilelab.dataplatformshaper.domain.common.db.interpreter

import cats.effect.*
import cats.effect.kernel.Resource.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.db.Session
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import doobie.*
import doobie.hikari.*
import com.zaxxer.hikari.HikariConfig
import org.eclipse.rdf4j.repository.RepositoryConnection

final case class JdbcSession(transactor: Resource[IO, HikariTransactor[IO]])
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
  def withTx[F[_]: Sync, T](func: RepositoryConnection => T): F[T] =
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
    dbType match
      case "jdbc" =>
        val transactor: Resource[IO, HikariTransactor[IO]] =
          for {
            hikariConfig <- Resource.pure {
              val config = new HikariConfig()
              config.setDriverClassName("org.h2.Driver")
              config.setJdbcUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
              config.setUsername(user)
              config.setPassword(pwd)
              config
            }
            xa <- HikariTransactor.fromHikariConfig[IO](hikariConfig)
          } yield xa
        JdbcSession(transactor)
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
      )(session => Sync[F].delay {}.void)
  end apply
end JdbcSession
