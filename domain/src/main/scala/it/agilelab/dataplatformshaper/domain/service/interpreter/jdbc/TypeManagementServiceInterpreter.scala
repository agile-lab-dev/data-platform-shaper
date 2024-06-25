package it.agilelab.dataplatformshaper.domain.service.interpreter.jdbc

import cats.data.EitherT
import cats.effect.Sync
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.db.interpreter.JdbcRepository
import it.agilelab.dataplatformshaper.domain.model.EntityType
import it.agilelab.dataplatformshaper.domain.model.schema.{
  jsonToSchema,
  schemaToJson
}
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  TypeManagementService
}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import scalikejdbc.*

class TypeManagementServiceInterpreter[F[_]: Sync](
  traitManagementService: TraitManagementServiceInterpreter[F]
) extends TypeManagementService[F]:
  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  val repository: JdbcRepository[F] = traitManagementService.repository

  override def create(
    entityType: EntityType
  ): F[Either[ManagementServiceError, Unit]] =

    val result = for
      exist <- EitherT(this.exist(entityType.name))
      _ <-
        if exist then
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              s"The entity with name ${entityType.name} already exists"
            )
          )
        else EitherT.rightT[F, ManagementServiceError](())
      schema = schemaToJson(entityType.schema)
      pgSchema = {
        val pgObject = new org.postgresql.util.PGobject()
        pgObject.setType("jsonb")
        pgObject.setValue(schema.noSpaces)
        pgObject
      }
      res <- EitherT(
        repository.session
          .withTx { implicit genericConnection =>
            val connection =
              repository.connectionToJdbcConnection(genericConnection)
            implicit val session: DBSession = DBSession(connection.connection)
            session.connection.setAutoCommit(false)
            val _ =
              sql"""
                insert into entity_type (name, data)
                values (${entityType.name}, $pgSchema)
                 """.update.apply()
          }
          .attempt
          .map {
            case Right(_) => Right(())
            case Left(e)  => Left(ManagementServiceError(e.getMessage))
          }
      )
    yield res

    result.value
  end create

  override def create(
    entityType: EntityType,
    inheritsFrom: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def read(
    instanceTypeName: String
  ): F[Either[ManagementServiceError, EntityType]] =
    val queryAction: F[Option[(String, String)]] = Sync[F].delay {
      DB readOnly { implicit session =>
        sql"""
             select name, data
             from entity_type
             where name = $instanceTypeName
           """.map(rs => (rs.string("name"), rs.string("data"))).single.apply()
      }
    }

    for
      queryResult <- queryAction.attempt
      result <- queryResult match
        case Right(Some(name: String, data: String)) =>
          val schemaConverted = jsonToSchema(data)
          schemaConverted match
            case Right(schema) =>
              logger
                .trace(s"Read of EntityType $instanceTypeName completed")
                .as(Right(EntityType(name, schema)))
            case Left(e) =>
              logger
                .trace(e)(
                  s"Parsing error of the schema of EntityType $instanceTypeName"
                )
                .as(Left(ManagementServiceError(e.getMessage)))
        case Left(e) =>
          logger
            .trace(e)(s"Failed to read the EntityType $instanceTypeName")
            .as(Left(ManagementServiceError(e.getMessage)))
        case _ =>
          logger
            .trace("Unknown values returned by query")
            .as(
              Left(
                ManagementServiceError(
                  s"Unknown values returned by reading the EntityType named $instanceTypeName"
                )
              )
            )
    yield result
  end read

  override def updateConstraints(
    entityType: EntityType
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def delete(
    instanceTypeName: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def exist(
    instanceTypeName: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val queryAction: F[Option[String]] = Sync[F].delay {
      DB readOnly { implicit session =>
        sql"""
             select id
             from entity_type
             where name = $instanceTypeName
           """.map(rs => rs.string("id")).single.apply()
      }
    }

    for
      queryResult <- queryAction.attempt
      result <- queryResult match
        case Right(existingTrait) =>
          val traitExists = existingTrait.isDefined
          logger
            .trace(s"Trait existence check completed")
            .as(Right(traitExists))
        case Left(e) =>
          logger
            .trace(e)("Failed to check trait existence")
            .as(Left(ManagementServiceError(e.getMessage)))
    yield result
  end exist

  override def list(): F[Either[ManagementServiceError, List[EntityType]]] = ???

end TypeManagementServiceInterpreter
