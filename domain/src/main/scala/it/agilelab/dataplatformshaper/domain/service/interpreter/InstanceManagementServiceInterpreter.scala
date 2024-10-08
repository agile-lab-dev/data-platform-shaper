package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.{*, given}
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
      entityType <- EitherT(typeManagementService.read(instanceTypeName))
      _ <-
        if entityType.traits.contains("MappingTarget")
        then
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              s"Cannot directly create an instance of a MappingTarget"
            )
          )
        else EitherT.rightT[F, ManagementServiceError](())
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
          Left[ManagementServiceError, String](
            ManagementServiceError(
              s"The EntityType $instanceTypeName does not exist"
            )
          ).pure[F]
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
      fe <- EitherT(
        fetchEntityFieldsAndTypeName(logger, repository, instanceId)
          .map(Right[ManagementServiceError, (String, List[(String, String)])])
      )
      _ <- traceT(s"Retrieved fields and type name $fe")
      entityType: EntityType <- EitherT(typeManagementService.read(fe(0)))
      _ <- traceT(s"Retrieved the entity type $entityType with name ${fe(0)} ")
      tuple <- EitherT[F, ManagementServiceError, Tuple](
        fieldsToTuple(logger, repository, fe(1), entityType.schema)
          .map(Right[ManagementServiceError, Tuple])
      )
      _ <- traceT(s"Loaded the tuple $tuple")
      entity <- EitherT[F, ManagementServiceError, Entity](
        Right[ManagementServiceError, Entity](
          Entity(instanceId, entityType.name, tuple)
        ).pure[F]
      )
      _ <- traceT(s"About to return the entity $entity")
    } yield entity

    (for {
      exist <- EitherT(exist(instanceId))
      r <-
        if exist then res
        else
          EitherT(
            Left[ManagementServiceError, Entity](
              ManagementServiceError(
                s"The instance with id $instanceId does not exist"
              )
            ).pure[F]
          )
    } yield r).value
  end read

  override def update(
    instanceId: String,
    values: Tuple
  ): F[Either[ManagementServiceError, String]] =
    val res: F[Either[ManagementServiceError, String]] = (for {
      _ <- traceT(s"About to remove the instance $instanceId")
      entity <- EitherT(read(instanceId))
      hasTrait <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          entity.entityTypeName,
          "MappingTarget"
        )
      )
      _ <-
        if hasTrait then
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              s"This instance with id $instanceId because its type ${entity.entityTypeName} is also a MappingTarget"
            )
          )
        else EitherT.rightT[F, ManagementServiceError](())
      result <- EitherT(
        updateInstanceNoCheck(
          logger,
          typeManagementService,
          this,
          instanceId,
          values
        )
      )
    } yield result).value

    EitherT(exist(instanceId)).flatMapF { exist =>
      if exist then res
      else
        Left(
          ManagementServiceError(
            s"This instance with id $instanceId does not exist"
          )
        ).pure[F]
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
      _ <- repository.removeAndInsertStatements(List.empty[Statement], stmts)
    } yield ()).map(_ => Right[ManagementServiceError, Unit](()))

    val checkLinkedInstancesQuery =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |SELECT (count(*) as ?count) WHERE {
         |        BIND(iri("${ns.getName}$instanceId") as ?instanceId1)
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
        repository
          .evaluateQuery(checkLinkedInstancesQuery)
          .map(ibs =>
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
          Left[ManagementServiceError, Unit](
            ManagementServiceError(
              s"This instance with id $instanceId cannot be deleted because has linked instances"
            )
          ).pure[F]
        else if exist then res
        else
          Left[ManagementServiceError, Unit](
            ManagementServiceError(
              s"This instance with id $instanceId cannot be deleted because does not exist"
            )
          ).pure[F]
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
    res.map(res => {
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
         |        FILTER NOT EXISTS { ?m ns:mappedBy ?i  }
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
        entities <- EitherT(ids.map(id => read(id)).sequence.map(_.sequence))
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
      (if query.trim.isEmpty then Either.right(None)
       else
         Either.catchNonFatal(Some(generateSearchPredicate("i", query)))
      ).left
        .map(t =>
          ManagementServiceError(s"Invalid search predicate: ${t.getMessage}")
        )
        .pure[F]

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

    val statements = statement(
      triple(
        iri(ns, instanceId1),
        iri(linkType.getNamespace, linkType),
        iri(ns, instanceId2)
      ),
      L2
    ) :: linkType.getInverse.fold(Nil: List[Statement])(inverse =>
      List(
        statement(
          triple(
            iri(ns, instanceId2),
            iri(linkType.getNamespace, inverse),
            iri(ns, instanceId1)
          ),
          L2
        )
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
          res
            .map(ibs =>
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
                Left[ManagementServiceError, Unit](
                  ManagementServiceError(
                    s"Linking $instanceId1 to $instanceId2 with relationship $linkType: invalid relationship"
                  )
                ).pure[F]
              end if
            )
            .flatten
        else
          if !exist1
          then
            Left[ManagementServiceError, Unit](
              ManagementServiceError(
                s"This instance with id $instanceId1 cannot be linked because does not exist"
              )
            ).pure[F]
          else
            if !exist2
            then
              Left[ManagementServiceError, Unit](
                ManagementServiceError(
                  s"This instance with id $instanceId2 cannot be linked because does not exist"
                )
              ).pure[F]
            else
              Left[ManagementServiceError, Unit](
                ManagementServiceError(
                  s"This instance with id $instanceId1 or $instanceId2 cannot be linked because does not exist"
                )
              ).pure[F]
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
    val statements = statement(
      triple(
        iri(ns, instanceId1),
        iri(linkType.getNamespace, linkType),
        iri(ns, instanceId2)
      ),
      L2
    ) :: linkType.getInverse.fold(Nil: List[Statement])(inverse =>
      List(
        statement(
          triple(
            iri(ns, instanceId2),
            iri(linkType.getNamespace, inverse),
            iri(ns, instanceId1)
          ),
          L2
        )
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
          repository
            .removeAndInsertStatements(List.empty, statements)
            .map(_ => Right[ManagementServiceError, Unit](()))
        else
          if !exist1 then
            Left[ManagementServiceError, Unit](
              ManagementServiceError(
                s"This instance with id $instanceId1 cannot be unlinked because does not exist"
              )
            ).pure[F]
          else
            if !exist2 then
              Left[ManagementServiceError, Unit](
                ManagementServiceError(
                  s"This instance with id $instanceId1 cannot be unlinked because does not exist"
                )
              ).pure[F]
            else
              Left[ManagementServiceError, Unit](
                ManagementServiceError(
                  s"This instance with id $instanceId1 or $instanceId2 cannot be unlinked because does not exist"
                )
              ).pure[F]
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
          repository
            .evaluateQuery(query)
            .map(
              _.map(bs =>
                iri(bs.getBinding("linked").getValue.stringValue()).getLocalName
              ).toList
            )
            .map(Right[ManagementServiceError, List[String]])
        else
          Left[ManagementServiceError, List[String]](
            ManagementServiceError(
              s"This instance with id $instanceId does not exist"
            )
          ).pure[F]
      )
    } yield res).value
  end linked

end InstanceManagementServiceInterpreter
