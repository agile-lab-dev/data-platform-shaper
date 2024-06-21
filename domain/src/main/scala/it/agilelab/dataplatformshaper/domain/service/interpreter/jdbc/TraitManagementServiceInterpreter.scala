package it.agilelab.dataplatformshaper.domain.service.interpreter.jdbc

import cats.data.EitherT
import cats.effect.Sync
import scalikejdbc.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.common.db.Repository
import it.agilelab.dataplatformshaper.domain.common.db.interpreter.JdbcRepository
import it.agilelab.dataplatformshaper.domain.model.{Relationship, Trait}
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  TraitManagementService
}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class TraitManagementServiceInterpreter[F[_]: Sync](
  genericRepository: Repository[F]
) extends TraitManagementService[F]:

  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  val repository: JdbcRepository[F] = genericRepository match
    case repo: JdbcRepository[F] => repo
    case _ => throw new IllegalArgumentException("Expected JdbcRepository")

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
        case None             => EitherT.rightT[F, ManagementServiceError](true)

      fatherId <-
        if fatherExist && traitDefinition.inheritsFrom.isDefined then
          EitherT(this.getTraitIdFromName(traitDefinition.inheritsFrom.get))
        else if traitDefinition.inheritsFrom.isDefined then
          EitherT.leftT[F, Long](
            ManagementServiceError(
              s"Father trait ${traitDefinition.inheritsFrom.getOrElse("")} does not exist"
            )
          )
        else EitherT.rightT[F, ManagementServiceError](0L)

      _ <- EitherT(
        repository.session
          .withTx { implicit genericConnection =>
            val connection =
              repository.connectionToJdbcConnection(genericConnection)
            implicit val session: DBSession = DBSession(connection.connection)
            session.connection.setAutoCommit(false)
            val traitId = insertTrait.updateAndReturnGeneratedKey.apply()
            if fatherExist && traitDefinition.inheritsFrom.isDefined then
              val insertRelationship =
                sql"""
              insert into relationship (subject_id, object_id, name)
              values ($traitId, $fatherId, 'subClassOf')
              """
              val _ = insertRelationship.update.apply()
          }
          .attempt
          .map {
            case Right(_) => Right(())
            case Left(e)  => Left(ManagementServiceError(e.getMessage))
          }
      )
    } yield ()).value
  end create

  private def hasLinkedTraits(
    traitName: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val queryAction = Sync[F].delay {
      DB.readOnly { implicit session =>
        sql"""
          select count(*) as c
          from trait t
          join relationship r on t.id = r.subject_id or t.id = r.object_id
          where t.name = $traitName
        """.map(rs => rs.int("c")).single.apply().getOrElse(0) > 0
      }
    }

    queryAction
      .map { count =>
        Right(count)
      }
      .handleErrorWith { e =>
        Sync[F]
          .delay {
            logger.trace(e)("Failed to check for linked traits")
          }
          .as(Left(ManagementServiceError(e.getMessage)))
      }
  end hasLinkedTraits

  override def delete(
    traitName: String
  ): F[Either[ManagementServiceError, Unit]] =
    val result = for
      _ <-
        if traitName.equals("MappingSource") || traitName.equals(
            "MappingTarget"
          )
        then
          EitherT.leftT[F, Unit](
            ManagementServiceError(s"Cannot delete trait $traitName")
          )
        else EitherT.rightT[F, ManagementServiceError](())
      exist <- EitherT(this.exist(traitName))
      _ <-
        if exist then EitherT.rightT[F, ManagementServiceError](())
        else
          EitherT.leftT[F, Unit](
            ManagementServiceError(s"Trait $traitName does not exist")
          )
      // TODO: Create the method entityHasTrait as seen in rdf4j/trms to check if an EntityType has this trait
      hasLinks <- EitherT(hasLinkedTraits(traitName))
      _ <-
        if hasLinks then
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              s"The trait $traitName is part of a relationship"
            )
          )
        else EitherT.rightT[F, ManagementServiceError](())
      traitID <- EitherT(this.getTraitIdFromName(traitName))
      _ <- EitherT
        .liftF(
          repository.session
            .withTx { implicit genericConnection =>
              val connection =
                repository.connectionToJdbcConnection(genericConnection)
              implicit val session: DBSession = DBSession(connection.connection)
              session.connection.setAutoCommit(false)
              val removeFatherRelationshipAction =
                sql"""
            delete from relationship
            where subject_id = $traitID and name = 'subClassOf'
             """.update.apply()
              val removeTraitAction =
                sql"""
            delete from trait
            where name = $traitName
             """.update.apply()
              (removeFatherRelationshipAction, removeTraitAction)
            }
            .attempt
            .map {
              case Right(_) => Right(())
              case Left(e)  => Left(ManagementServiceError(e.getMessage))
            }
        )
        .void
    yield ()
    result.value
  end delete

  private def getTraitIdFromName(
    name: String
  ): F[Either[ManagementServiceError, Long]] =
    val queryAction: F[Set[String]] = Sync[F].delay {
      DB.readOnly { implicit session =>
        sql"""
          select id
          from trait
          where name = $name
        """.map(rs => rs.string("id")).list.apply().toSet
      }
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
      DB.readOnly { implicit session =>
        sql"""
          select name
          from trait
          where name = $traitName
        """.map(rs => rs.string("name")).single.apply()
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

  override def list(): F[Either[ManagementServiceError, List[String]]] =
    val queryAction: F[List[String]] = Sync[F].delay {
      DB.readOnly { implicit session =>
        sql"""
          select name
          from trait
        """.map(rs => rs.string("name")).list.apply()
      }
    }

    queryAction
      .map { names =>
        Right(names)
      }
      .handleErrorWith { e =>
        Sync[F]
          .delay {
            logger.trace(e)("Failed to list traits")
          }
          .as(Left(ManagementServiceError(e.getMessage)))
      }
  end list

  override def exist(
    traitNames: Set[String]
  ): F[Either[ManagementServiceError, Set[(String, Boolean)]]] =
    val queryAction: F[Set[String]] = Sync[F].delay {
      DB.readOnly { implicit session =>
        sql"""
          select name
          from trait
          where name in (${traitNames.map(name => s"'$name'").mkString(", ")})
        """.map(rs => rs.string("name")).list.apply().toSet
      }
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
    traitName1: String,
    linkType: Relationship,
    traitName2: String
  ): F[Either[ManagementServiceError, Unit]] =
    val result = for {
      _ <- traceT(
        s"About to link $traitName1 with $traitName2 using the relationship $linkType"
      )
      exist1 <- EitherT(exist(traitName1))
      exist2 <- EitherT(exist(traitName2))
      _ <-
        if exist1 && exist2 then
          for
            id1 <- EitherT(this.getTraitIdFromName(traitName1))
            id2 <- EitherT(this.getTraitIdFromName(traitName2))
            res <- EitherT.liftF(repository.session.withTx {
              genericConnection =>
                val connection =
                  repository.connectionToJdbcConnection(genericConnection)
                implicit val session: DBSession =
                  DBSession(connection.connection)
                session.connection.setAutoCommit(false)
                val insertRelationship =
                  sql"""
                  insert into relationship (subject_id, object_id, name)
                  values ($id1, $id2, ${linkType.toString})
                   """
                insertRelationship.update.apply()
            })
          yield res
        else if !exist1 then
          EitherT.leftT[F, Int](
            ManagementServiceError(s"The trait $traitName1 does not exist")
          )
        else
          EitherT.leftT[F, Int](
            ManagementServiceError(s"The trait $traitName2 does not exist")
          )
    } yield ()
    result.value
  end link

  override def unlink(
    trait1Name: String,
    linkType: Relationship,
    trait2Name: String
  ): F[Either[ManagementServiceError, Unit]] = ???

  override def linked(
    traitName: String,
    linkType: Relationship
  ): F[Either[ManagementServiceError, List[String]]] =
    val result =
      for
        _ <- traceT(
          s"Looking for linked traits for trait $traitName and relationship kind $linkType"
        )
        exist <- EitherT(this.exist(traitName))
        res <-
          if exist then
            for
              traitId <- EitherT(this.getTraitIdFromName(traitName))
              res <- EitherT.liftF(Sync[F].delay {
                DB.readOnly { implicit session =>
                  val query =
                    sql"""
                      select t.name
                      from relationship r
                      join trait t on t.id = r.object_id
                      where subject_id = $traitId
                    """
                  query.map(rs => rs.string("name")).list.apply()
                }
              })
            yield res
          else
            EitherT.leftT[F, List[String]](
              ManagementServiceError(s"The trait $traitName does not exist")
            )
      yield res
    result.value
  end linked
end TraitManagementServiceInterpreter
