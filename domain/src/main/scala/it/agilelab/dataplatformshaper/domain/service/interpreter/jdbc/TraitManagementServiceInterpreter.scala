package it.agilelab.dataplatformshaper.domain.service.interpreter.jdbc

import cats.data.EitherT
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
    val insertTrait =
      sql"""
            insert into trait (name)
            values (${traitDefinition.traitName})
          """

    (for {
      exist <- EitherT(this.exist(traitDefinition.traitName))
      _ <- EitherT.fromEither(
        if exist then
          Left(
            ManagementServiceError(
              s"Trait ${traitDefinition.traitName} already exists"
            )
          )
        else Right(())
      )
      fatherExist <- traitDefinition.inheritsFrom match
        case Some(fatherName) => EitherT(this.exist(fatherName))
        case None => EitherT.rightT[F, ManagementServiceError](false)

      fatherId <-
        if fatherExist then
          EitherT(this.getTraitIdFromName(traitDefinition.inheritsFrom.get))
        else EitherT.rightT[F, ManagementServiceError](0L)

      _ <- EitherT.liftF(repository.session.withTx { genericConnection =>
        val connection =
          repository.connectionToJdbcConnection(genericConnection)
        implicit val session: DBSession = DBSession(connection.connection)
        val traitId = insertTrait.updateAndReturnGeneratedKey.apply()
        if fatherExist then
          val insertRelationship =
            sql"""
              insert into relationship (trait1_id, trait2_id, name)
              values ($traitId, $fatherId, 'subClassOf')
               """
          val _ = insertRelationship.update.apply()

        traitId
      })
    } yield ()).value
  end create

  override def create(
    bulkTraitsCreationRequest: BulkTraitsCreationRequest
  ): F[BulkTraitsCreationResponse] = ???

  override def delete(
    traitName: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  private def getTraitIdFromName(
    name: String
  ): F[Either[ManagementServiceError, Long]] =
    val queryAction: F[Set[String]] = Sync[F].delay {
      implicit val session: DBSession = DBSession(connection)
      sql"""
        select id
        from trait
        where name = $name
      """.map(rs => rs.string("id")).list.apply().toSet
    }

    queryAction.map { ids =>
      if ids.isEmpty then Left(ManagementServiceError("Trait not found"))
      else if ids.size > 1 then
        Left(ManagementServiceError("Multiple traits found"))
      else Right(ids.head.toLong)
    }
  end getTraitIdFromName

  override def exist(
    traitName: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val queryAction: F[Option[String]] = Sync[F].delay {
      implicit val session: DBSession = DBSession(connection)
      sql"""
          select name
          from trait
          where name = $traitName
        """.map(rs => rs.string("name")).single.apply()
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

  override def list(): F[Either[ManagementServiceError, List[String]]] = ???

  override def exist(
    traitNames: Set[String]
  ): F[Either[ManagementServiceError, Set[(String, Boolean)]]] =
    val queryAction: F[Set[String]] = Sync[F].delay {
      implicit val session: DBSession = DBSession(connection)
      sql"""
        select name
        from trait
        where name in (${traitNames.mkString(", ")})
      """.map(rs => rs.string("name")).list.apply().toSet
    }

    for
      queryResult <- queryAction.attempt
      result <- queryResult match
        case Right(existingTraits) =>
          val traitExistenceSet =
            traitNames.map(name => (name, existingTraits.contains(name)))
          logger
            .trace(s"Trait existence check completed")
            .as(Right(traitExistenceSet))
        case Left(e) =>
          logger
            .trace(e)("Failed to check trait existence")
            .as(Left(ManagementServiceError(e.getMessage)))
    yield result
  end exist

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
