package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.*
import it.agilelab.dataplatformshaper.domain.model.NS.*
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.l1.{*, given}
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
import it.agilelab.dataplatformshaper.domain.service.{
  InstanceManagementService,
  ManagementServiceError,
  TypeManagementService
}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, triple}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID
import scala.language.{implicitConversions, postfixOps}

class InstanceManagementServiceInterpreter[F[_]: Sync](
    typeManagementService: TypeManagementService[F]
) extends InstanceManagementService[F]
    with InstanceManagementServiceInterpreterCommonFunctions[F]:

  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  val repository: KnowledgeGraph[F] = typeManagementService.repository

  override def create(
      instanceTypeName: String,
      values: Tuple
  ): F[Either[ManagementServiceError, String]] =
    val entityId = UUID.randomUUID().toString
    (for {
      exist <- EitherT(typeManagementService.exist(instanceTypeName))
      result <- EitherT {
        if exist
        then
          createInstanceNoCheck(
            logger,
            typeManagementService,
            entityId,
            instanceTypeName,
            values,
            List.empty,
            List.empty
          )
        else
          summon[Applicative[F]]
            .pure(
              Left[ManagementServiceError, String](
                ManagementServiceError.NonExistentInstanceTypeError(
                  instanceTypeName
                )
              )
            )
        end if
      }
      _ <- traceT(
        s"Instance $result created with type name $instanceTypeName and values $values"
      )
    } yield result).value
  end create

  override def read(
      instanceId: String
  ): F[Either[ManagementServiceError, Entity]] =

    val res = for {
      _ <- traceT(s"About to read instance with id: $instanceId")
      fe <- EitherT[
        F,
        ManagementServiceError,
        (String, List[(String, String)])
      ](
        summon[Functor[F]].map(
          fetchEntityFieldsAndTypeName(logger, repository, instanceId)
        )(
          Right[ManagementServiceError, (String, List[(String, String)])]
        )
      )
      _ <- traceT(s"Retrieved fields and type name $fe")
      entityType: EntityType <- EitherT(typeManagementService.read(fe(0)))
      _ <- traceT(s"Retrieved the entity type $entityType with name ${fe(0)} ")
      tuple <- EitherT[F, ManagementServiceError, Tuple](
        summon[Functor[F]].map(
          fieldsToTuple(logger, repository, fe(1), entityType.schema)
        )(
          Right[ManagementServiceError, Tuple]
        )
      )
      _ <- traceT(s"Loaded the tuple $tuple")
      entity <- EitherT[F, ManagementServiceError, Entity](
        summon[Applicative[F]].pure(
          Right[ManagementServiceError, Entity](
            Entity(instanceId, entityType.name, tuple)
          )
        )
      )
      _ <- traceT(s"About to return the entity $entity")
    } yield entity

    (for {
      exist <- EitherT(exist(instanceId))
      r <-
        if exist then res
        else
          EitherT(
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Entity](
                ManagementServiceError.NonExistentInstanceError(instanceId)
              )
            )
          )
    } yield r).value

  end read

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.=="
    )
  )
  override def update(
      instanceId: String,
      values: Tuple
  ): F[Either[ManagementServiceError, String]] =
    val statementsToRemove: F[List[Statement]] = fetchStatementsForInstance(
      repository,
      instanceId
    )
    val res: F[Either[ManagementServiceError, String]] = (for {
      _ <- EitherT.liftF(
        logger.trace(s"About to remove the instance $instanceId")
      )
      stmts <- EitherT.liftF(statementsToRemove)
      _ <- EitherT.liftF(
        logger.trace(
          s"Statements for removing the previous version \n${stmts.mkString("\n")}"
        )
      )
      instanceType = iri(
        stmts
          .filter(_.getPredicate == NS.ISCLASSIFIEDBY)
          .head
          .getObject
          .stringValue()
      ).getLocalName
      entityType <- EitherT(typeManagementService.read(instanceType))
      _ <- EitherT[F, ManagementServiceError, Unit](
        summon[Applicative[F]].pure(
          cueValidate(entityType.schema, values).leftMap(errors =>
            InstanceValidationError(errors)
          )
        )
      )
      _ <- EitherT.liftF(
        logger.trace(s"$instanceId is classified by $instanceType")
      )
      result <- EitherT(
        createInstanceNoCheck(
          logger,
          typeManagementService,
          instanceId,
          instanceType,
          values,
          List.empty,
          stmts
        )
      )
    } yield result).value

    EitherT(exist(instanceId)).flatMapF { exist =>
      if exist then res
      else
        summon[Applicative[F]].pure(
          Left(ManagementServiceError.NonExistentInstanceError(instanceId))
        )
    }.value
  end update

  override def delete(
      instanceId: String
  ): F[Either[ManagementServiceError, Unit]] =
    val res: F[Either[ManagementServiceError, Unit]] = (for {
      _ <- logger.trace(s"About to remove the instance $instanceId")
      stmts <- fetchStatementsForInstance(repository, instanceId)
      _ <- logger.trace(
        s"About to remove the statements \n${stmts.mkString("\n")}"
      )
      _ <- repository.removeAndInsertStatements(
        List.empty[Statement],
        stmts
      )
    } yield ()).map(_ => Right[ManagementServiceError, Unit](()))

    val checkLinkedInstancesQuery =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |SELECT (count(*) as ?count) WHERE {
         |        BIND(iri("${ns.getName}$instanceId") as ?instanceId)
         |        ?instanceId1 ?rel ?instanceId2 . 
         |        ?instanceId1 ns:isClassifiedBy ?type1 .
         |        ?instanceId2 ns:isClassifiedBy ?type2 .
         |        ?type1 ns:hasTrait ?trait1 .
         |        ?type2 ns:hasTrait ?trait2 .
         |        ?trait1 rdfs:subClassOf* ?tr1 .
         |        ?trait2 rdfs:subClassOf* ?tr2 .
         |        ?tr1 ?rel ?tr2 .
         |  }
         |""".stripMargin

    val linkedInstancesExisting: F[Either[ManagementServiceError, Boolean]] =
      logger.trace(
        s"About to run the query $checkLinkedInstancesQuery to check if the instance $instanceId has linked instances"
      ) *>
        summon[Functor[F]]
          .map(repository.evaluateQuery(checkLinkedInstancesQuery))(ibs =>
            val count = ibs
              .map(bs => bs.getBinding("count").getValue.stringValue())
              .toList
              .headOption
            count.map(_.toInt > 1).getOrElse(false)
          )
          .map(Right[ManagementServiceError, Boolean](_))

    (for {
      exist <- EitherT(exist(instanceId))
      linkedInstancesExisting <- EitherT(linkedInstancesExisting)
      _ <- traceT(
        s"Instance $instanceId has linked instances: $linkedInstancesExisting"
      )
      r <- EitherT(
        if linkedInstancesExisting then
          summon[Applicative[F]].pure(
            Left[ManagementServiceError, Unit](
              ManagementServiceError.InstanceHasLinkedInstancesError(instanceId)
            )
          )
        else if exist then res
        else
          summon[Applicative[F]].pure(
            Left[ManagementServiceError, Unit](
              ManagementServiceError.NonExistentInstanceError(instanceId)
            )
          )
      )
    } yield r).value

  end delete

  override def exist(
      instanceId: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val res = repository.evaluateQuery(s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |SELECT ?entity ?entityType WHERE {
         |    BIND(iri("${ns.getName}$instanceId") as ?entity)
         |    ?entity ns:isClassifiedBy ?entityType
         |  }
         |""".stripMargin)
    summon[Functor[F]].map(res)(res => {
      val count = res.toList.length
      if count > 0
      then Right[ManagementServiceError, Boolean](true)
      else Right[ManagementServiceError, Boolean](false)
    })
  end exist

  override def list(
      instanceTypeName: String,
      predicate: Option[SearchPredicate],
      returnEntities: Boolean,
      limit: Option[Int]
  ): F[Either[ManagementServiceError, List[String | Entity]]] =
    val limitClause = limit.map(l => s"LIMIT $l").getOrElse("")

    val query =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |SELECT DISTINCT ?i  WHERE {
         |    {
         |        BIND(iri("${ns.getName}$instanceTypeName") as ?entityType)
         |        ?i rdf:type ns:Entity .
         |        ?i ns:isClassifiedBy ?entityType .
         |    }
         |    ${predicate.fold("")(_.querySegment)}
         |}
         |$limitClause
         |""".stripMargin

    if !returnEntities then
      logger.trace(
        s"About to evaluate the query $query for retrieving a list of instance ids"
      ) *>
        repository
          .evaluateQuery(query)
          .map(
            _.map(bs => iri(bs.getValue("i").stringValue()).getLocalName).toList
          )
          .map(Right[ManagementServiceError, List[String]])
    else
      (for {
        _ <- traceT(
          s"About to evaluate the query $query for retrieving a list of instance ids"
        )
        ids <- EitherT(
          repository
            .evaluateQuery(query)
            .map(
              _.map(bs =>
                iri(bs.getValue("i").stringValue()).getLocalName
              ).toList
            )
            .map(Right[ManagementServiceError, List[String]])
        )
        entities <- EitherT(
          summon[Functor[F]].map(ids.map(id => read(id)).sequence)(_.sequence)
        )
      } yield entities).value
    end if
  end list

  override def list(
      instanceTypeName: String,
      query: String,
      returnEntities: Boolean,
      limit: Option[Int]
  ): F[Either[ManagementServiceError, List[String | Entity]]] =
    val predicate: F[Either[ManagementServiceError, Option[SearchPredicate]]] =
      summon[Applicative[F]].pure(
        (if query.trim.isEmpty then Either.right(None)
         else Either.catchNonFatal(Some(generateSearchPredicate(query)))).left
          .map(t => ManagementServiceError.InvalidSearchPredicate(t.getMessage))
      )

    (for {
      pred <- EitherT(predicate)
      list <- EitherT(list(instanceTypeName, pred, returnEntities, limit))
    } yield list).value
  end list

  override def link(
      instanceId1: String,
      linkType: Relationship,
      instanceId2: String
  ): F[Either[ManagementServiceError, Unit]] =
    val query =
      s"""
       |PREFIX ns:  <${ns.getName}>
       |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
       |PREFIX owl: <http://www.w3.org/2002/07/owl#>
       |SELECT (COUNT(*) as ?count) WHERE {
       |        BIND(iri("${ns.getName}$instanceId1") as ?instanceId1)
       |        BIND(iri("${ns.getName}$instanceId2") as ?instanceId2)
       |        ?instanceId1 ns:isClassifiedBy ?type1 .
       |        ?instanceId2 ns:isClassifiedBy ?type2 .
       |        ?type1 ns:hasTrait ?trait1 .
       |        ?type2 ns:hasTrait ?trait2 .
       |        ?trait1 rdfs:subClassOf* ?tr1 .
       |        ?trait2 rdfs:subClassOf* ?tr2 .
       |        ?tr1 <${linkType.getNamespace}$linkType> ?tr2 .
       |  }
       |""".stripMargin
    val res =
      logger.trace(
        s"Querying about $instanceId1 and $instanceId2 if linked using $linkType"
      ) *> logger.trace(s"Using query $query") *> repository.evaluateQuery(
        query
      )

    val statements = List(
      statement(
        triple(
          iri(ns, instanceId1),
          iri(linkType.getNamespace, linkType),
          iri(ns, instanceId2)
        ),
        L3
      )
    )
    (for {
      _ <- traceT(
        s"About to link $instanceId1 with $instanceId2 using the relationship $linkType"
      )
      exist1 <- EitherT(exist(instanceId1))
      exist2 <- EitherT(exist(instanceId2))
      link <- EitherT(
        if exist1 && exist2
        then
          summon[Functor[F]]
            .map(res)(ibs =>
              val count = ibs
                .map(bs => bs.getBinding("count").getValue.stringValue())
                .toList
                .headOption
              val found = count === Some("1")
              if found then
                repository
                  .removeAndInsertStatements(statements, List.empty)
                  .map(_ => Right[ManagementServiceError, Unit](()))
              else
                summon[Applicative[F]].pure(
                  Left[ManagementServiceError, Unit](
                    InvalidLinkType(instanceId1, linkType, instanceId2)
                  )
                )
              end if
            )
            .flatten
        else
          if !exist1
          then
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](
                NonExistentInstanceError(instanceId1)
              )
            )
          else
            if !exist2
            then
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  NonExistentInstanceError(instanceId2)
                )
              )
            else
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  NonExistentInstanceError(instanceId1)
                )
              )
            end if
          end if
      )
    } yield link).value
  end link

  override def unlink(
      instanceId1: String,
      linkType: Relationship,
      instanceId2: String
  ): F[Either[ManagementServiceError, Unit]] =
    val statements = List(
      statement(
        triple(
          iri(ns, instanceId1),
          iri(linkType.getNamespace, linkType),
          iri(ns, instanceId2)
        ),
        L3
      )
    )
    (for {
      _ <- traceT(
        s"About to unlink $instanceId1 with $instanceId2 using the relationship $linkType"
      )
      exist1 <- EitherT(exist(instanceId1))
      exist2 <- EitherT(exist(instanceId2))
      res <- EitherT(
        if exist1 && exist2 then
          summon[Functor[F]].map(
            repository.removeAndInsertStatements(
              List.empty,
              statements
            )
          )(_ => Right[ManagementServiceError, Unit](()))
        else
          if !exist1 then
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](
                NonExistentInstanceError(instanceId1)
              )
            )
          else
            if !exist2 then
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  NonExistentInstanceError(instanceId2)
                )
              )
            else
              summon[Applicative[F]].pure(
                Left[ManagementServiceError, Unit](
                  NonExistentInstanceError(instanceId1)
                )
              )
            end if
          end if
      )
    } yield res).value
  end unlink

  override def linked(
      instanceId: String,
      linkType: Relationship
  ): F[Either[ManagementServiceError, List[String]]] =
    val query =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |SELECT ?linked WHERE {
         |  BIND(iri("${ns.getName}$instanceId") as ?instance)
         |    ?instance <${linkType.getNamespace}$linkType> ?linked .
         |  }
         |""".stripMargin

    (for {
      _ <- traceT(
        s"Looking for linked instances for instance $instanceId and relationship kind $linkType"
      )
      exist <- EitherT(exist(instanceId))
      _ <- traceT(s"Looking for linked instances with the query: $query")
      res <- EitherT(
        if exist then
          summon[Functor[F]].map(
            repository
              .evaluateQuery(query)
              .map(
                _.map(bs =>
                  iri(
                    bs.getBinding("linked").getValue.stringValue()
                  ).getLocalName
                ).toList
              )
          )(Right[ManagementServiceError, List[String]])
        else
          summon[Applicative[F]].pure(
            Left[ManagementServiceError, List[String]](
              NonExistentInstanceError(instanceId)
            )
          )
      )
    } yield res).value
  end linked

end InstanceManagementServiceInterpreter
