package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS
import it.agilelab.dataplatformshaper.domain.model.NS.{L2, L3, ns}
import it.agilelab.dataplatformshaper.domain.model.l1.{*, given}
import it.agilelab.dataplatformshaper.domain.model.mapping.{
  MappingDefinition,
  MappingKey
}
import it.agilelab.dataplatformshaper.domain.model.schema.{
  schemaToMapperSchema,
  tupleToMappedTuple,
  validateMappingTuple
}
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
import it.agilelab.dataplatformshaper.domain.service.{
  InstanceManagementService,
  ManagementServiceError,
  MappingManagementService,
  TypeManagementService
}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, literal, triple}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID
import scala.collection.mutable

class MappingManagementServiceInterpreter[F[_]: Sync](
    typeManagementService: TypeManagementService[F],
    instanceManagementService: InstanceManagementService[F]
) extends MappingManagementService[F]
    with InstanceManagementServiceInterpreterCommonFunctions[F]:

  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  val repository: KnowledgeGraph[F] = typeManagementService.repository

  override def create(
      mappingDefinition: MappingDefinition
  ): F[Either[ManagementServiceError, Unit]] =
    val key = mappingDefinition.mappingKey
    val mapper = mappingDefinition.mapper
    val sourceEntityTypeIri = iri(ns, key.sourceEntityTypeName)
    val targetEntityTypeIri = iri(ns, key.targetEntityTypeName)
    val mapperId = UUID.randomUUID().toString
    val mapperIri = iri(ns, mapperId)

    val mappedToTriple1 = triple(
      sourceEntityTypeIri,
      iri(mappedTo.getNamespace, s"${mappedTo: String}#${key.mappingName}"),
      targetEntityTypeIri
    )

    val mappedToTriple2 = triple(
      iri(mappedTo.getNamespace, s"${mappedTo: String}#${key.mappingName}"),
      iri(ns, "singletonPropertyOf"),
      iri(mappedTo.getNamespace, mappedTo)
    )

    val mappedToTriple3 = triple(
      iri(mappedTo.getNamespace, s"${mappedTo: String}#${key.mappingName}"),
      NS.MAPPEDBY,
      mapperIri
    )

    val initialStatements = List(
      statement(
        mappedToTriple1,
        L2
      ),
      statement(
        mappedToTriple2,
        L2
      ),
      statement(
        mappedToTriple3,
        L2
      )
    )

    (for {
      _ <- traceT(
        s"About to create a mapping named ${mappingDefinition.mappingKey.mappingName} with source ${key.sourceEntityTypeName}, target ${key.targetEntityTypeName} with the mapper tuple:\n ${mappingDefinition.mapper} "
      )
      _ <- EitherT(
        detectCycles(
          logger,
          repository,
          mappingDefinition.mappingKey.sourceEntityTypeName,
          mappingDefinition.mappingKey.targetEntityTypeName
        )
      )
      _ <- EitherT(
        summon[Functor[F]]
          .map(exist(key))(
            _.flatMap(exist =>
              if exist then
                Left[ManagementServiceError, Unit](
                  InvalidMappingError(
                    s"the mapping with name ${key.mappingName}, source type ${key.sourceEntityTypeName} and target type ${key.targetEntityTypeName} already exists"
                  )
                )
              else Right[ManagementServiceError, Unit](())
            )
          )
      )
      _ <- EitherT(
        summon[Functor[F]]
          .map(
            checkTraitForEntityType(
              logger,
              repository,
              key.sourceEntityTypeName,
              "MappingSource"
            )
          )(
            _.flatMap(exist =>
              if !exist then
                Left[ManagementServiceError, Unit](
                  InvalidMappingError(
                    s"in the mapping with name ${key.mappingName}, the source type ${key.sourceEntityTypeName} doesn't contain the trait MappingSource"
                  )
                )
              else Right[ManagementServiceError, Unit](())
            )
          )
      )
      _ <- EitherT(
        summon[Functor[F]]
          .map(
            checkTraitForEntityType(
              logger,
              repository,
              key.targetEntityTypeName,
              "MappingTarget"
            )
          )(
            _.flatMap(exist =>
              if !exist then
                Left[ManagementServiceError, Unit](
                  InvalidMappingError(
                    s"in the mapping with name ${key.mappingName}, the target type ${key.targetEntityTypeName} doesn't contain the trait MappingTarget"
                  )
                )
              else Right[ManagementServiceError, Unit](())
            )
          )
      )
      ttype <- EitherT(typeManagementService.read(key.targetEntityTypeName))
      _ <- EitherT(
        summon[Applicative[F]].pure(
          validateMappingTuple(mapper, ttype.schema).leftMap(error =>
            MapperInstanceValidationError(error)
          )
        )
      )
      stmts <- EitherT(
        summon[Applicative[F]].pure(
          emitStatementsForEntity(
            mapperId,
            ttype.name,
            mapper,
            schemaToMapperSchema(ttype.schema),
            L2
          )
        )
      )
      res <- EitherT(
        summon[Functor[F]].map(
          repository.removeAndInsertStatements(
            initialStatements ::: stmts,
            List.empty[Statement]
          )
        )(Right[ManagementServiceError, Unit])
      )
    } yield res).value
  end create

  def read(
      mappingKey: MappingKey
  ): F[Either[ManagementServiceError, MappingDefinition]] =
    val query =
      s"""
         |PREFIX ns: <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |SELECT DISTINCT ?predicate ?object WHERE {
         |   <${ns.getName}mappedTo#${mappingKey.mappingName}> ns:mappedBy ?mappedByValue .
         |   ns:${mappingKey.sourceEntityTypeName} <${ns.getName}mappedTo#${mappingKey.mappingName}> ns:${mappingKey.targetEntityTypeName} .
         |   ?mappedByValue ?predicate ?object
         |   FILTER(?predicate != rdf:type && ?predicate != ns:isClassifiedBy)
         |}
         |""".stripMargin

    val res = logger.trace(
      s"Reading the mapping with name ${mappingKey.mappingName} with the query:\n$query"
    ) *> repository.evaluateQuery(query)

    summon[Functor[F]].map(res) { result =>
      val pairs = Iterator
        .continually(result)
        .takeWhile(_.hasNext)
        .map(_.next())
        .map { bindingSet =>
          val obj = Option(bindingSet.getValue("object"))
            .map(_.stringValue())
            .getOrElse("")
          val pred = Option(bindingSet.getValue("predicate"))
            .map(_.stringValue())
            .getOrElse("")

          (iri(pred).getLocalName, obj)
        }
        .filter { case (pred, obj) => pred.nonEmpty && obj.nonEmpty }
        .toList

      pairs match
        case List(tuple1, tuple2) =>
          Right(MappingDefinition(mappingKey, (tuple1, tuple2)))
        case _ =>
          Left(
            MappingNotFoundError(
              s"Mapping with name ${mappingKey.mappingName} has not been found or does not have exactly two pairs"
            )
          )
    }
  end read

  def update(
      mappingKey: MappingKey,
      mapper: Tuple
  ): F[Either[ManagementServiceError, Unit]] =
    (for {
      mappings <- EitherT(
        getMappingsForEntityType(
          logger,
          typeManagementService,
          mappingKey.sourceEntityTypeName
        )
      )
      filteredMappings <- EitherT.fromOption[F](
        Some(mappings.filter { case (_, _, targetEntityType, _, _) =>
          targetEntityType.name.equals(mappingKey.targetEntityTypeName)
        }),
        MappingNotFoundError("No mappings found matching the criteria")
      )
      firstMapping <- EitherT.fromOption[F](
        filteredMappings.headOption,
        MappingNotFoundError("No mappings found matching the criteria")
      )
      (_, _, _, pairs, mappingId) = firstMapping
      oldStatements = pairs.toList.map { case (key: String, value: String) =>
        val mappedToTriple1 = triple(
          iri(ns, mappingId),
          iri(ns, key),
          literal(value)
        )
        statement(mappedToTriple1, L2)
      }
      newStatements = mapper.toList.map { case (key: String, value: String) =>
        val mappedToTriple1 = triple(
          iri(ns, mappingId),
          iri(ns, key),
          literal(value)
        )
        statement(mappedToTriple1, L2)
      }
      _ <- EitherT.liftF(
        repository.removeAndInsertStatements(newStatements, oldStatements)
      )
      roots <- EitherT.liftF(
        getRoots(logger, repository, mappingKey.sourceEntityTypeName)
      )
      rawInstanceIdsList <- roots.traverse { root =>
        EitherT(
          instanceManagementService.list(
            instanceTypeName = root,
            predicate = "",
            returnEntities = false,
            limit = None
          )
        ).map(_.collect { case s: String => s })
      }
      instanceIds <- EitherT.liftF(
        summon[Applicative[F]].pure(rawInstanceIdsList.flatten.distinct)
      )
      _ <- EitherT.liftF(instanceIds.traverse(id => updateMappedInstances(id)))
      _ <- EitherT.liftF(logger.trace(s"Selected mapping last string: $pairs"))
    } yield ()).value
  end update

  def delete(
      mappingKey: MappingKey
  ): F[Either[ManagementServiceError, Unit]] =
    (for {
      existInstances <- EitherT(
        existMappedInstances(
          logger,
          repository,
          Some(mappingKey.sourceEntityTypeName),
          Some(mappingKey.mappingName),
          Some(mappingKey.targetEntityTypeName)
        )
      )
      _ <-
        if (existInstances)
          EitherT.leftT[F, Unit](ExistingInstancesError(mappingKey.mappingName))
        else EitherT.rightT[F, ManagementServiceError](())
      isMappingSource <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          mappingKey.sourceEntityTypeName,
          "MappingSource"
        )
      )
      isMappingTarget <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          mappingKey.sourceEntityTypeName,
          "MappingTarget"
        )
      )
      _ <-
        if (!isMappingSource || isMappingTarget)
          EitherT.leftT[F, Unit](
            InvalidMappingError("Source is not a root of the mapping")
          )
        else EitherT.rightT[F, ManagementServiceError](())
      sourceMappings <- EitherT(
        getMappingsForEntityType(
          logger,
          typeManagementService,
          mappingKey.sourceEntityTypeName
        )
      )
      filteredMappings = sourceMappings.filter {
        case (_, sourceEntityType, targetEntityType, _, _) =>
          sourceEntityType.name.equals(
            mappingKey.sourceEntityTypeName
          ) && targetEntityType.name.equals(mappingKey.targetEntityTypeName)
      }
      (
        mappingName,
        sourceEntityType,
        targetEntityType,
        mapper,
        mapperId
      ) = filteredMappings.head
      firstMappingDefinition = MappingDefinition(
        MappingKey(
          mappingName,
          sourceEntityType.name,
          targetEntityType.name
        ),
        mapper
      )
      _ <- EitherT(
        deleteMappedInstances(
          logger,
          repository,
          typeManagementService,
          firstMappingDefinition,
          mapperId
        )
      )
      children <- EitherT.liftF(
        getRelations(logger, repository, mappingKey.targetEntityTypeName, false)
      )
      _ <- EitherT(
        recursiveDelete(
          logger,
          repository,
          mappingKey.targetEntityTypeName,
          typeManagementService
        )
      )
      _ <- EitherT.liftF(logger.trace(s"Selected mapping last string:"))
    } yield ()).value
  end delete

  override def exist(
      mapperKey: MappingKey
  ): F[Either[ManagementServiceError, Boolean]] =
    val query =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT distinct ?mr WHERE {
         |   ?mr ns:singletonPropertyOf ns:mappedTo .
         |   ns:${mapperKey.sourceEntityTypeName} ?mr ns:${mapperKey.targetEntityTypeName} .
         |   FILTER(STR(?mr) = "${ns.getName}mappedTo#${mapperKey.mappingName}")
         |}
         |""".stripMargin
    val res = logger.trace(
      s"Checking the mapping existence with name ${mapperKey.mappingName} with the query:\n$query"
    ) *> repository.evaluateQuery(query)
    summon[Functor[F]].map(res)(res =>
      val count = res.toList.length
      if count > 0 then Right[ManagementServiceError, Boolean](true)
      else Right[ManagementServiceError, Boolean](false)
      end if
    )
  end exist

  override def createMappedInstances(
      sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]] =
    (for {
      sourceInstance <- EitherT(
        instanceManagementService.read(sourceInstanceId)
      )
      existInstances <- EitherT(
        existMappedInstances(
          logger,
          repository,
          Some(sourceInstance.entityTypeName),
          None,
          None
        )
      )
      _ <-
        if (existInstances)
          EitherT.leftT[F, Unit](ExistingInstancesError(" "))
        else
          EitherT.rightT[F, ManagementServiceError](())
      mappings <- EitherT(
        getMappingsForEntityType(
          logger,
          typeManagementService,
          sourceInstance.entityTypeName
        )
      )
      ids <- EitherT(
        summon[Functor[F]].map(Traverse[List].sequence(mappings.map(mapping =>
          val tuple: Either[ManagementServiceError, Tuple] = tupleToMappedTuple(
            sourceInstance.values,
            mapping(1).schema,
            mapping(3),
            mapping(2).schema
          ).leftMap(e => MapperInstanceValidationError(e))
          val targetInstanceId = UUID.randomUUID().toString
          val sourceEntityIri = iri(ns, sourceInstanceId)
          val targetEntityIri = iri(ns, targetInstanceId)
          val mapperIri = iri(ns, mapping(4))
          val mappedToTriple1 = triple(
            sourceEntityIri,
            iri(mappedTo.getNamespace, s"${mappedTo: String}#${mapping(0)}"),
            targetEntityIri
          )
          val mappedToTriple2 = triple(
            iri(mappedTo.getNamespace, s"${mappedTo: String}#${mapping(0)}"),
            iri(ns, "singletonPropertyOf"),
            iri(mappedTo.getNamespace, mappedTo)
          )
          val mappedToTriple3 = triple(
            iri(mappedTo.getNamespace, s"${mappedTo: String}#${mapping(0)}"),
            NS.MAPPEDBY,
            mapperIri
          )
          val initialStatements = List(
            statement(mappedToTriple1, L3),
            statement(mappedToTriple2, L3),
            statement(mappedToTriple3, L3)
          )
          summon[Functor[F]].map(
            tuple
              .map(
                createInstanceNoCheck(
                  logger,
                  typeManagementService,
                  targetInstanceId,
                  mapping(2).name,
                  _,
                  initialStatements,
                  List.empty
                )
              )
              .sequence
          )(_.flatten)
        )))(_.sequence)
      )
      _ <- EitherT(
        summon[Functor[F]]
          .map(ids.map(id => createMappedInstances(id)).sequence)(_.sequence)
      )
    } yield ()).value
  end createMappedInstances

  override def updateMappedInstances(
      sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]] =
    (for {
      mappings <- EitherT(
        getMappingsForEntity(
          logger,
          typeManagementService,
          instanceManagementService,
          sourceInstanceId
        )
      )
      _ <- traceT(s"Retrieved mappings: $mappings")
      ids <- EitherT(
        summon[Functor[F]].map(Traverse[List].sequence(mappings.map(mapping =>
          val tuple: Either[ManagementServiceError, Tuple] = tupleToMappedTuple(
            mapping(1).values,
            mapping(0).schema,
            mapping(4),
            mapping(2).schema
          ).leftMap(e => MapperInstanceValidationError(e))
          summon[Functor[F]].map(
            tuple
              .map(
                updateInstanceNoCheck(
                  logger,
                  typeManagementService,
                  instanceManagementService,
                  mapping(3).entityId,
                  _
                )
              )
              .sequence
          )(_.flatten)
        )))(_.sequence)
      )
      _ <- traceT(s"Instances to update: $ids")
      _ <- EitherT(
        summon[Functor[F]]
          .map(ids.map(id => updateMappedInstances(id)).sequence)(_.sequence)
      )
    } yield ()).value
  end updateMappedInstances

  override def deleteMappedInstances(
      sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]] =
    val instancesToDelete = mutable.Stack.empty[String]

    def getMappedInstancesToDelete(
        sourceInstanceId: String
    ): F[Either[ManagementServiceError, Unit]] =
      (for {
        mappings <- EitherT(
          getMappingsForEntity(
            logger,
            typeManagementService,
            instanceManagementService,
            sourceInstanceId
          )
        )
        ids <- EitherT.liftF(
          summon[Applicative[F]].pure(mappings.map(mapping =>
            instancesToDelete.push(mapping(3).entityId)
            mapping(3).entityId: String
          ))
        )
        _ <- EitherT(
          summon[Functor[F]]
            .map(ids.map(id => getMappedInstancesToDelete(id)).sequence)(
              _.sequence
            )
        )
      } yield ()).value
    end getMappedInstancesToDelete

    import cats.syntax.all.*
    (for {
      stack <- EitherT.liftF(
        summon[Functor[F]].map(getMappedInstancesToDelete(sourceInstanceId))(
          _ => instancesToDelete
        )
      )
      _ <- EitherT(
        summon[Functor[F]].map(
          stack.popAll.toList
            .map(id => instanceManagementService.delete(id))
            .sequence
        )(_.sequence)
      )
    } yield ()).value
  end deleteMappedInstances
end MappingManagementServiceInterpreter
