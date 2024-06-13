package it.agilelab.dataplatformshaper.domain.service.interpreter.rdf4j

import cats.*
import cats.data.*
import cats.effect.*
import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.common.db.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS.{L2, ns}
import it.agilelab.dataplatformshaper.domain.model.mapping.{
  MappingDefinition,
  MappingKey
}
import it.agilelab.dataplatformshaper.domain.model.schema.{
  getPathQuery,
  schemaToMapperSchema,
  tupleToMappedTuple,
  validateMappingTuple
}
import it.agilelab.dataplatformshaper.domain.model.{*, given}
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  MappingManagementService
}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, literal, triple}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID
import scala.collection.mutable
import scala.util.Try

class MappingManagementServiceInterpreter[F[_]: Sync](
  typeManagementService: TypeManagementServiceInterpreter[F],
  instanceManagementService: InstanceManagementServiceInterpreter[F]
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

    val additionalSourcesReferencesStatements =
      mappingDefinition.additionalSourcesReferences
        .flatMap(pair =>
          val nieIri = iri(ns, UUID.randomUUID().toString)
          List(
            statement(
              triple(nieIri, NS.INSTANCEREFERENCENAME, literal(pair(0))),
              L2
            ),
            statement(
              triple(nieIri, NS.INSTANCEREFERENCEEXPRESSION, literal(pair(1))),
              L2
            ),
            statement(
              triple(
                iri(
                  mappedTo.getNamespace,
                  s"${mappedTo: String}#${key.mappingName}"
                ),
                NS.WITHNAMEDINSTANCEREFERENCEEXPRESSION,
                nieIri
              ),
              L2
            )
          )
        )
        .toList

    val initialStatements = List(
      statement(mappedToTriple1, L2),
      statement(mappedToTriple2, L2),
      statement(mappedToTriple3, L2)
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
        exist(key).map(
          _.flatMap(exist =>
            if exist then
              Left[ManagementServiceError, Unit](
                ManagementServiceError(
                  s"the mapping with name ${key.mappingName}, source type ${key.sourceEntityTypeName} and target type ${key.targetEntityTypeName} already exists"
                )
              )
            else Right[ManagementServiceError, Unit](())
          )
        )
      )
      _ <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          key.sourceEntityTypeName,
          "MappingSource"
        )
          .map(
            _.flatMap(exist =>
              if !exist then
                Left[ManagementServiceError, Unit](
                  ManagementServiceError(
                    s"in the mapping with name ${key.mappingName}, the source type ${key.sourceEntityTypeName} does not contain the trait MappingSource"
                  )
                )
              else Right[ManagementServiceError, Unit](())
            )
          )
      )
      _ <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          key.targetEntityTypeName,
          "MappingTarget"
        )
          .map(
            _.flatMap(exist =>
              if !exist then
                Left[ManagementServiceError, Unit](
                  ManagementServiceError(
                    s"In the mapping with name ${key.mappingName}, the target type ${key.targetEntityTypeName} does not contain the trait MappingTarget"
                  )
                )
              else Right[ManagementServiceError, Unit](())
            )
          )
      )
      ttype <- EitherT(typeManagementService.read(key.targetEntityTypeName))
      _ <- EitherT(
        validateMappingTuple(mapper, ttype.schema)
          .leftMap(error =>
            ManagementServiceError(s"The mapper instance is invalid: $error")
          )
          .pure[F]
      )
      stmts <- EitherT(
        emitStatementsForEntity(
          mapperId,
          ttype.name,
          mapper,
          schemaToMapperSchema(ttype.schema),
          L2
        ).pure[F]
      )
      res <- EitherT(
        repository
          .removeAndInsertStatements(
            initialStatements ::: stmts ::: additionalSourcesReferencesStatements,
            List.empty[Statement]
          )
          .map(Right[ManagementServiceError, Unit])
      )
    } yield res).value
  end create

  private def readAdditionalReferences(
    mappingKey: MappingKey
  ): F[Map[String, String]] =
    val query =
      s"""
         | PREFIX ns: <${ns.getName}>
         | PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         | SELECT DISTINCT ?referenceName ?referenceExpression WHERE {
         | <${ns.getName}mappedTo#${mappingKey.mappingName}> ns:withNamedInstanceReferenceExpression ?referenceId .
         | ?referenceId ns:instanceReferenceName ?referenceName .
         | ?referenceId ns:instanceReferenceExpression ?referenceExpression
         | }
         |""".stripMargin
    val res = logger.trace(
      s"Reading the additional references of mapping with name ${mappingKey.mappingName}"
    ) *> repository.evaluateQuery(query)
    res
      .map(_.map(bs =>
        val referenceName = bs.getValue("referenceName").stringValue()
        val referenceExpression =
          bs.getValue("referenceExpression").stringValue()
        referenceName -> referenceExpression
      ))
      .map(_.toMap)
  end readAdditionalReferences

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
    for {
      iteratorBs <- res
      pairs = iteratorBs
        .map(bs =>
          val obj =
            Option(bs.getValue("object")).map(_.stringValue).getOrElse("")
          val pred =
            Option(bs.getValue("predicate")).map(_.stringValue).getOrElse("")
          (iri(pred).getLocalName, obj)
        )
        .filter { case (pred, obj) => pred.nonEmpty && obj.nonEmpty }
        .toList
      additionalReferences <- readAdditionalReferences(mappingKey)
      res = Tuple.fromArray(pairs.toArray) match
        case tuple if tuple.toArray.length > 0 =>
          Right(MappingDefinition(mappingKey, tuple, additionalReferences))
        case _ =>
          Left(
            ManagementServiceError(
              s"Mapping with name ${mappingKey.mappingName} has not been found"
            )
          )
    } yield res
  end read

  private def prepareReferenceTuple(
    splitPath: List[String],
    initialInstanceId: String
  ): F[Either[ManagementServiceError, Tuple]] =
    val queryBody =
      Try(getPathQuery(splitPath, initialInstanceId)).toEither.leftMap(t =>
        ManagementServiceError(t.getMessage)
      )
    val finalQuery = queryBody.map((query, trueFinalInstanceId) => s"""
         | PREFIX ns:   <${ns.getName}>
         | PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         | PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         | PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         | SELECT DISTINCT ?$trueFinalInstanceId WHERE {
         | $query
         | }
         |""".stripMargin)
    finalQuery.fold(
      l =>
        val res: Either[ManagementServiceError, Tuple] = Left(l)
        res.pure[F]
      ,
      query =>
        (for {
          res <- EitherT.liftF(repository.evaluateQuery(query))
          listOfInstanceIds <- EitherT.fromEither(
            queryBody.map((_, finalInstanceId) =>
              res
                .map(bs =>
                  iri(
                    ns,
                    bs.getValue(finalInstanceId).stringValue()
                  ).getLocalName
                )
                .toList
            )
          )
          tupleEither <-
            if listOfInstanceIds.length.equals(1) then
              for {
                entity <- EitherT(
                  instanceManagementService.read(listOfInstanceIds.head)
                )
                entityValues = entity.values
              } yield entityValues
            else
              val res: EitherT[F, ManagementServiceError, Tuple] =
                EitherT.leftT(
                  ManagementServiceError(
                    "Found a number of instances different from one"
                  )
                )
              res
        } yield tupleEither).value
    )
  end prepareReferenceTuple

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
        ManagementServiceError("No mappings found matching the criteria")
      )
      firstMapping <- EitherT.fromOption[F](
        filteredMappings.headOption,
        ManagementServiceError("No mappings found matching the criteria")
      )
      (_, _, _, pairs, mappingId) = firstMapping
      oldStatements = pairs.toList.map { case (key: String, value: String) =>
        val mappedToTriple1 =
          triple(iri(ns, mappingId), iri(ns, key), literal(value))
        statement(mappedToTriple1, L2)
      }
      newStatements = mapper.toList.map { case (key: String, value: String) =>
        val mappedToTriple1 =
          triple(iri(ns, mappingId), iri(ns, key), literal(value))
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
            query = "",
            returnEntities = false,
            limit = None: Option[Int]
          )
        ).map(_.collect { case s: String => s })
      }
      instanceIds <- EitherT.liftF(rawInstanceIdsList.flatten.distinct.pure[F])
      _ <- EitherT.liftF(instanceIds.traverse(id => updateMappedInstances(id)))
      _ <- EitherT.liftF(logger.trace(s"Selected mapping last string: $pairs"))
    } yield ()).value
  end update

  def delete(mappingKey: MappingKey): F[Either[ManagementServiceError, Unit]] =
    (for {
      existingInstances <- EitherT(
        queryMappedInstances(
          logger,
          repository,
          Some(mappingKey.sourceEntityTypeName),
          Some(mappingKey.mappingName),
          Some(mappingKey.targetEntityTypeName)
        )
      )
      _ <-
        if existingInstances.nonEmpty
        then
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              s"Cannot delete the mapping, there are associated instances"
            )
          )
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
        if !(isMappingSource && !isMappingTarget)
        then
          EitherT.leftT[F, Unit](
            ManagementServiceError("Source is not a root of the mapping")
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
      (mappingName, sourceEntityType, targetEntityType, mapper, mapperId) =
        filteredMappings.head
      firstMappingDefinition = MappingDefinition(
        MappingKey(mappingName, sourceEntityType.name, targetEntityType.name),
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
    res.map(res =>
      val count = res.toList.length
      if count > 0 then Right[ManagementServiceError, Boolean](true)
      else Right[ManagementServiceError, Boolean](false)
      end if
    )
  end exist

  private def splitPath(path: String): List[String] =
    def loop(listPaths: List[String]): List[String] =
      listPaths match
        case head1 :: head2 :: tail
            if head1.contains(".find(") && !head1.contains(")") =>
          head1 + "/" + head2 :: loop(tail)
        case head :: tail => head :: loop(tail)
        case Nil          => Nil
    end loop
    val splitPath = path.split("/").toList
    loop(splitPath)
  end splitPath

  override def createMappedInstances(
    sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]] =
    val completedOperations: scala.collection.mutable.Stack[Boolean] =
      scala.collection.mutable.Stack.empty[Boolean]
    def createMappedInstancesNoCheck(
      sourceInstanceId: String
    ): F[Either[ManagementServiceError, Unit]] =
      (for {
        sourceInstance <- EitherT(
          instanceManagementService.read(sourceInstanceId)
        )
        mappings <- EitherT(
          getMappingsForEntityType(
            logger,
            typeManagementService,
            sourceInstance.entityTypeName
          )
        )
        // format: off
        ids <- EitherT(
          Traverse[List].sequence(mappings.map(mapping =>
            val tempTuple: EitherT[F, ManagementServiceError, Tuple] =
              for {
                mappedInstances <- EitherT.liftF(getMappedInstancesReferenceExpressions(logger, typeManagementService, mapping._1, mapping._2.name))
                mappedQueries <- EitherT.liftF(mappedInstances.map { case (name, path) =>
                  val paths = splitPath(path)
                  val preparedQuery = prepareReferenceTuple(paths, sourceInstanceId)
                  preparedQuery.map(_.map((name, _)))
                }.toList.sequence.pure[F])
                tuplesMapped <- EitherT.liftF(mappedQueries)
                finalTuplesMapped <- EitherT.fromEither(tuplesMapped.traverse(identity).map(_.toMap))
                tupleResult <- EitherT(tupleToMappedTuple(
                  sourceInstance.values,
                  mapping(1).schema,
                  mapping(3),
                  mapping(2).schema,
                  finalTuplesMapped
                ).leftMap(e => ManagementServiceError(s"The mapper instance is invalid: $e")).pure[F])
              } yield tupleResult
            val tuple = tempTuple.value
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
              statement(mappedToTriple1, L2),
              statement(mappedToTriple2, L2),
              statement(mappedToTriple3, L2)
            )
            (for {
              targetInstanceOption <- EitherT(
                readTargetInstance(sourceInstanceId, mapping._1)
              )
              result <-
                if targetInstanceOption.isDefined
                then
                  completedOperations.push(false)
                  EitherT.liftF(
                    targetInstanceOption.get.entityId.pure[F]
                  )
                else
                  for {
                    _ <- EitherT.liftF(
                      completedOperations.push(true).pure[F]
                    )
                    tmpCreatedInstance <- EitherT(
                      tuple.map(
                        bs => bs.map(
                          cs => createInstanceNoCheck(
                            logger,
                            typeManagementService,
                            targetInstanceId,
                            mapping(2).name,
                            cs,
                            initialStatements,
                            List.empty)
                        )
                      )
                    )
                    createdInstance <- EitherT(tmpCreatedInstance)
                  } yield createdInstance
            } yield result).value
          )).map(_.sequence)
        )
        _ <- EitherT(
          ids.map(id => createMappedInstancesNoCheck(id)).sequence.map(_.sequence)
        )
      } yield ()).value
    end createMappedInstancesNoCheck

    (for {
      sourceInstance <- EitherT(
        instanceManagementService.read(sourceInstanceId)
      )
      isMappingSource <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          sourceInstance.entityTypeName,
          "MappingSource"
        )
      )
      isMappingTarget <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          sourceInstance.entityTypeName,
          "MappingTarget"
        )
      )
      _ <-
        if !(isMappingSource && !isMappingTarget)
        then
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              "This instance is also a MappingTarget, it's probably not a root in the mapping DAG"
            )
          )
        else EitherT.rightT[F, ManagementServiceError](())
      res <- EitherT(createMappedInstancesNoCheck(sourceInstanceId))
      _ <-
        if completedOperations.contains(true)
        then EitherT.rightT[F, ManagementServiceError](())
        else
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              s"The instances for this mapping have already been created"
            )
          )
    } yield res).value
  end createMappedInstances

  def readMappedInstances(
      sourceInstanceId: String
  ): F[Either[ManagementServiceError, List[
    ((EntityType, Entity), String, (EntityType, Entity))
  ]]] =
    def readMappedInstancesNoCheck(
        sourceInstanceId: String
    ): F[Either[ManagementServiceError, List[
      ((EntityType, Entity), String, (EntityType, Entity))
    ]]] =
      (for {
        entities <- EitherT(
          getMappingsForEntity(
            logger,
            typeManagementService,
            instanceManagementService,
            sourceInstanceId
          ).map(ml => ml.map(_.map(m => ((m(0), m(1)), m(5), (m(2), m(3))))))
        )
        _ <- traceT(s"Read instances to update: $entities")
        entities <- EitherT(
          entities
              .map(e => readMappedInstancesNoCheck(e(2)(1).entityId))
            .sequence
            .map(_.sequence.map(_.flatten).map(l => entities ::: l))
        )
      } yield entities).value
    end readMappedInstancesNoCheck

    (for {
      sourceInstance <- EitherT(
        instanceManagementService.read(sourceInstanceId)
      )
      isMappingSource <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          sourceInstance.entityTypeName,
          "MappingSource"
        )
      )
      isMappingTarget <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          sourceInstance.entityTypeName,
          "MappingTarget"
        )
      )
      _ <-
        if !(isMappingSource && !isMappingTarget)
        then
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              "This instance is also a MappingTarget, it's probably not a root in the mapping DAG"
            )
          )
        else EitherT.rightT[F, ManagementServiceError](())
      res <- EitherT(readMappedInstancesNoCheck(sourceInstanceId))
    } yield res).value
  end readMappedInstances

  override def updateMappedInstances(
      sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]] =
    def updateMappedInstancesNoCheck(
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
          Traverse[List].sequence(mappings.map(mapping =>
            val mappingNameSplit = mapping._6.split("#")
            val mappingName = if mappingNameSplit.length.equals(2) then mappingNameSplit(1) else ""
            val resIds = for {
                mappedInstances <- EitherT.liftF(
                  getMappedInstancesReferenceExpressions(
                    logger, 
                    typeManagementService,
                    mappingName, 
                    mapping(0).name
                  )
                )
                mappedQueries <- EitherT.liftF(mappedInstances.map { case (name, path) =>
                  val paths = splitPath(path)
                  val preparedQuery = prepareReferenceTuple(paths, sourceInstanceId)
                  preparedQuery.map(_.map((name, _)))
                }.toList.sequence.pure[F])
                tuplesMapped <- EitherT.liftF(mappedQueries)
                finalTuplesMapped <- EitherT.fromEither(tuplesMapped.traverse(identity).map(_.toMap))
                tupleResult <- EitherT(tupleToMappedTuple(
                  mapping(1).values,
                  mapping(0).schema,
                  mapping(4),
                  mapping(2).schema,
                  finalTuplesMapped
                ).leftMap(e => ManagementServiceError(s"The mapper instance is invalid: $e")).pure[F])
                res <- EitherT(
                  updateInstanceNoCheck(
                    logger, 
                    typeManagementService, 
                    instanceManagementService, 
                    mapping(3).entityId, 
                    tupleResult
                  )
                )
              } yield res
            resIds.value
          )).map(_.sequence)
        )
        _ <- traceT(s"Instances to update: $ids")
        _ <- EitherT(
          ids.map(id => updateMappedInstancesNoCheck(id)).sequence.map(
            _.sequence
          )
        )
      } yield ()).value
    end updateMappedInstancesNoCheck

    (for {
      sourceInstance <- EitherT(
        instanceManagementService.read(sourceInstanceId)
      )
      isMappingSource <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          sourceInstance.entityTypeName,
          "MappingSource"
        )
      )
      isMappingTarget <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          sourceInstance.entityTypeName,
          "MappingTarget"
        )
      )
      _ <-
        if !(isMappingSource && !isMappingTarget)
        then
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              "This instance is also a MappingTarget, it's probably not a root in the mapping DAG"
            )
          )
        else EitherT.rightT[F, ManagementServiceError](())
      res <- EitherT(updateMappedInstancesNoCheck(sourceInstanceId))
    } yield res).value
  end updateMappedInstances

  private def getTargetEntityType(
      sourceEntityTypeName: String
  ): F[Either[ManagementServiceError, List[EntityType]]] =
    val query =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT DISTINCT ?targetEntityType ?mappedTo WHERE {
         |  ?sourceEntityType ?mappedTo ?targetEntityType .
         |  ?sourceEntityType rdf:type ns:EntityType .
         |  ?targetEntityType rdf:type ns:EntityType .
         |  FILTER(STR(?sourceEntityType) = "${ns.getName}$sourceEntityTypeName")
         |  FILTER(STRSTARTS(STR(?mappedTo), "${ns.getName}mappedTo#"))
         |}
         |""".stripMargin

    repository
      .evaluateQuery(query)
      .flatMap { queryResults =>
        queryResults.toList.traverse { bs =>
          typeManagementService.read(
            iri(bs.getValue("targetEntityType").stringValue()).getLocalName
          )
        }
      }
      .map(_.sequence)
      .map(
        _.leftMap(_ =>
          ManagementServiceError("Error in reading getTargetEntityType")
        )
      )
      .map(_.map(_.toList))

  end getTargetEntityType

  private def deleteOnlyMappedInstances(
      initialEntity: String
  ): F[Either[ManagementServiceError, Set[EntityType]]] =
    def loop(
        stack: mutable.Stack[String],
        accumulated: Set[EntityType]
    ): F[Either[ManagementServiceError, Set[EntityType]]] =
      if stack.isEmpty then Right(accumulated).pure[F]
      else
        val head = stack.pop()
        getTargetEntityType(head).flatMap {
          case Left(error) => Left(error).pure[F]
          case Right(entityTypes) =>
            entityTypes
              .traverse { entityType =>
                if !accumulated.exists(_.name.equals(entityType.name)) then
                  stack.push(entityType.name)
                val res = for {
                  ids <- EitherT(
                    instanceManagementService
                      .list(entityType.name, None, false, None)
                  )
                  stringIds = ids.collect { case s: String => s }
                  _ <- stringIds.traverse_(id =>
                    EitherT(instanceManagementService.delete(id))
                  )
                } yield ()
                res.value
              }
              .flatMap(_ => loop(stack, accumulated ++ entityTypes))
        }
    end loop
    loop(mutable.Stack(initialEntity), Set.empty)
  end deleteOnlyMappedInstances

  override def deleteMappedInstances(
      sourceInstanceId: String
  ): F[Either[ManagementServiceError, Unit]] =

    def deleteMappedInstancesNoCheck(
        sourceInstanceId: String
    ): F[Either[ManagementServiceError, Unit]] =
      val instancesToDelete = mutable.Stack.empty[String]
      val statementsToRemove = mutable.Stack.empty[Statement]
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
          entity <- EitherT(instanceManagementService.read(sourceInstanceId))
          mappedInstances <- EitherT(
            queryMappedInstances(
              logger,
              repository,
              Some(entity.entityTypeName),
              None,
              None
            )
          )
          filteredInstances = mappedInstances.filter(mappedInstance =>
            iri(mappedInstance(0)).getLocalName.equals(sourceInstanceId)
          )
          filteredStatements = filteredInstances.map(filteredInstance =>
            statement(
              iri(filteredInstance(0)),
              iri(filteredInstance(1)),
              iri(filteredInstance(2)),
              L2
            )
          )
          _ <- EitherT.liftF(
            statementsToRemove.pushAll(filteredStatements).pure[F]
          )
          ids <- EitherT.liftF(
            mappings.map(mapping =>
              instancesToDelete.push(mapping(3).entityId)
              mapping(3).entityId: String
            ).pure[F]
          )
          _ <- EitherT(
            ids.map(id => getMappedInstancesToDelete(id)).sequence.map(
              _.sequence
            )
          )
        } yield ()).value
      end getMappedInstancesToDelete

      (for {
        stack <- EitherT.liftF(
          getMappedInstancesToDelete(sourceInstanceId).map(
            _ => instancesToDelete
          )
        )
        _ <- EitherT.liftF(
          repository.removeAndInsertStatements(
            List.empty[Statement],
            statementsToRemove.toList
          )
        )
        entity <- EitherT(instanceManagementService.read(sourceInstanceId))
        _ <- EitherT(deleteOnlyMappedInstances(entity.entityTypeName))
      } yield ()).value
    end deleteMappedInstancesNoCheck

    (for {
      sourceInstance <- EitherT(
        instanceManagementService.read(sourceInstanceId)
      )
      isMappingSource <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          sourceInstance.entityTypeName,
          "MappingSource"
        )
      )
      isMappingTarget <- EitherT(
        checkTraitForEntityType(
          logger,
          repository,
          sourceInstance.entityTypeName,
          "MappingTarget"
        )
      )
      _ <-
        if !(isMappingSource && !isMappingTarget)
        then
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              "This instance is also a MappingTarget, it's probably not a root in the mapping DAG"
            )
          )
        else EitherT.rightT[F, ManagementServiceError](())
      res <- EitherT(deleteMappedInstancesNoCheck(sourceInstanceId))
    } yield res).value
  end deleteMappedInstances

  override def readTargetInstance(
      sourceInstanceId: String,
      mappingName: String
  ): F[Either[ManagementServiceError, Option[Entity]]] =
    val query =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT DISTINCT ?targetId WHERE {
         |  ?sourceId ?mapping ?targetId .
         |  ?sourceId rdf:type ns:Entity .
         |  ?targetId rdf:type ns:Entity .
         |  FILTER(STR(?sourceId) = "${ns.getName}$sourceInstanceId")
         |  FILTER(STR(?mapping) = "${ns.getName}mappedTo#$mappingName")
         |}
         |""".stripMargin
    val res = logger.trace(
      s"Reading the target instance with source $sourceInstanceId linked by mapping $mappingName"
    ) *> repository.evaluateQuery(query)
    val response = res.flatMap {
      case results if results.hasNext =>
        val targetId = results.next().getValue("targetId").toString
        instanceManagementService
          .read(targetId)
          .map(_.map(entity => Option(entity)))
      case _ =>
        Right(None).pure[F]
    }
    response
  end readTargetInstance

end MappingManagementServiceInterpreter
