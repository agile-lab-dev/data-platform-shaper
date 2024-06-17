package it.agilelab.dataplatformshaper.domain.service.interpreter.jdbc

import cats.effect.Sync
import scalikejdbc.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.db.Repository
import it.agilelab.dataplatformshaper.domain.common.db.interpreter.JdbcRepository
import it.agilelab.dataplatformshaper.domain.model.{
  BulkTraitsCreationRequest,
  BulkTraitsCreationResponse,
  Relationship,
  Trait
}
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  TraitManagementService
}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.sql.Connection

class TraitManagementServiceInterpreter[F[_]: Sync](
  genericRepository: Repository[F]
) extends TraitManagementService[F]:

  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  val repository: JdbcRepository[F] = genericRepository match
    case repo: JdbcRepository[F] => repo
    case _ => throw new IllegalArgumentException("Expected JdbcRepository")

  val connection: Connection = repository.session.connection.connection

  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  override def create(
    traitDefinition: Trait
  ): F[Either[ManagementServiceError, Unit]] =
    val insertAction: F[Long] = Sync[F].delay {
      implicit val session: DBSession = DBSession(connection)
      sql"""
        insert into trait (name)
        values (${traitDefinition.traitName})
      """.updateAndReturnGeneratedKey.apply()
    }

    for
      generatedKey <- insertAction.attempt
      result <- generatedKey match
        case Right(key) =>
          logger.info(s"Inserted record with generated ID: $key").as(Right(()))
        case Left(e) =>
          logger
            .error(e)(s"Failed to insert trait: ${traitDefinition.traitName}")
            .as(Left(ManagementServiceError(e.getMessage)))
    yield result
  end create

  override def create(
    bulkTraitsCreationRequest: BulkTraitsCreationRequest
  ): F[BulkTraitsCreationResponse] = ???

  override def delete(
    traitName: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def exist(
    traitName: String
  ): F[Either[ManagementServiceError, Boolean]] = ???

  override def list(): F[Either[ManagementServiceError, List[String]]] = ???

  override def exist(
    traitNames: Set[String]
  ): F[Either[ManagementServiceError, Set[(String, Boolean)]]] = ???

  override def link(
    trait1Name: String,
    linkType: Relationship,
    traitName2: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def unlink(
    trait1Name: String,
    linkType: Relationship,
    trait2Name: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def linked(
    traitName: String,
    linkType: Relationship
  ): F[Either[ManagementServiceError, List[String]]] = ???
end TraitManagementServiceInterpreter
