package it.agilelab.dataplatformshaper.domain.service.interpreter
import cats.*
import cats.data.EitherT
import cats.effect.*
import cats.implicits.*
import io.circe.Json
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS.{L2, ns}
import it.agilelab.dataplatformshaper.domain.model.Relationship.mappedTo
import it.agilelab.dataplatformshaper.domain.model.mapping.{
  MappingDefinition,
  MappingKey
}
import it.agilelab.dataplatformshaper.domain.model.schema.DataType.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.model.schema.parsing.FoldingPhase
import it.agilelab.dataplatformshaper.domain.model.schema.parsing.FoldingPhase.*
import it.agilelab.dataplatformshaper.domain.model.schema.{
  DataType,
  Schema,
  cueValidate,
  schemaToMapperSchema,
  unfoldTuple
}
import it.agilelab.dataplatformshaper.domain.model.{
  Entity,
  EntityType,
  NS,
  Relationship,
  given
}
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
import it.agilelab.dataplatformshaper.domain.service.{
  InstanceManagementService,
  ManagementServiceError,
  TypeManagementService
}
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, literal, triple}
import org.eclipse.rdf4j.model.vocabulary.RDF
import org.eclipse.rdf4j.model.{IRI, Literal, Statement}
import org.typelevel.log4cats.Logger

import java.time.{LocalDate, ZonedDateTime}
import java.util.UUID
import scala.annotation.tailrec
import scala.collection.mutable

trait InstanceManagementServiceInterpreterCommonFunctions[F[_]: Sync]:

  self: InstanceManagementServiceInterpreter[F] |
    MappingManagementServiceInterpreter[F] =>

  def createInstanceNoCheck(
    logger: Logger[F],
    typeManagementService: TypeManagementService[F],
    entityId: String,
    instanceTypeName: String,
    tuple: Tuple,
    additionalStatementsToAdd: List[Statement],
    statementsToRemove: List[Statement]
  ): F[Either[ManagementServiceError, String]] =

    given Logger[F] = logger

    val getSchema: F[Either[ManagementServiceError, Schema]] =
      Functor[F].map(typeManagementService.read(instanceTypeName))(
        _.map(_.schema)
      )

    (for {
      schema <- EitherT[F, ManagementServiceError, Schema](getSchema)
      _ <- traceT(s"Retrieved schema $schema for type name $instanceTypeName")
      _ <- EitherT[F, ManagementServiceError, Unit](
        cueValidate(schema, tuple)
          .leftMap(errors =>
            ManagementServiceError(s"Instance validation error" :: errors)
          )
          .pure[F]
      )
      stmts <- EitherT[F, ManagementServiceError, List[Statement]](
        emitStatementsForEntity(entityId, instanceTypeName, tuple, schema)
          .pure[F]
      )
      _ <- traceT(s"Statements emitted ${stmts.mkString("\n")}")
      id <- EitherT[F, ManagementServiceError, String](
        typeManagementService.repository
          .removeAndInsertStatements(
            additionalStatementsToAdd ::: stmts,
            statementsToRemove
          )
          .map(_ => Right[ManagementServiceError, String](entityId))
      )
      _ <- traceT(
        s"Statements emitted creating the instance $id:\n${stmts.mkString("\n")}\n"
      )
    } yield id).value
  end createInstanceNoCheck

  @SuppressWarnings(
    Array("scalafix:DisableSyntax.var", "scalafix:DisableSyntax.defaultArgs")
  )
  def emitStatementsForEntity(
    entityId: String,
    instanceTypeName: String,
    tuple: Tuple,
    schema: Schema,
    ontologyLevel: IRI = L2
  ): Either[ManagementServiceError, List[Statement]] =

    val entity = iri(ns, entityId)
    var statements = statement(
      triple(entity, NS.ISCLASSIFIEDBY, iri(ns, instanceTypeName)),
      L2
    ) ::
      statement(triple(entity, RDF.TYPE, NS.ENTITY), L2) :: Nil
    val previousEntityIriStack = mutable.Stack(entity)
    var currentEntityIri = entity

    @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
    def internalEmitStatements(
      currentPath: String,
      tpe: DataType,
      value: Any,
      foldingPhase: FoldingPhase
    ): Unit =
      tpe match
        case StringType(mode, _) =>
          val lit =
            mode match
              case Required | Repeated => literal(value.asInstanceOf[String])
              case Nullable =>
                value
                  .asInstanceOf[Option[String]]
                  .fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            ontologyLevel
          ) :: statements
        case IntType(mode, _) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[Int])
              case Nullable =>
                value.asInstanceOf[Option[Int]].fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            ontologyLevel
          ) :: statements
        case DateType(mode, _) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[LocalDate])
              case Nullable =>
                value
                  .asInstanceOf[Option[LocalDate]]
                  .fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            ontologyLevel
          ) :: statements
        case JsonType(mode) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[Json])
              case Nullable =>
                value
                  .asInstanceOf[Option[Json]]
                  .fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            ontologyLevel
          ) :: statements
        case TimestampType(mode, _) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[ZonedDateTime].toString)
              case Nullable =>
                value
                  .asInstanceOf[Option[ZonedDateTime]]
                  .map(_.toString)
                  .fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            ontologyLevel
          ) :: statements
        case DoubleType(mode, _) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[Double])
              case Nullable =>
                value
                  .asInstanceOf[Option[Double]]
                  .fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            ontologyLevel
          ) :: statements
        case FloatType(mode, _) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[Float])
              case Nullable =>
                value.asInstanceOf[Option[Float]].fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            ontologyLevel
          ) :: statements
        case LongType(mode, _) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[Long])
              case Nullable =>
                value.asInstanceOf[Option[Long]].fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            ontologyLevel
          ) :: statements
        case BooleanType(mode, _) =>
          val lit =
            mode match
              case Required | Repeated =>
                literal(value.asInstanceOf[Boolean])
              case Nullable =>
                value
                  .asInstanceOf[Option[Boolean]]
                  .fold(literal("null"))(literal)
            end match
          statements = statement(
            triple(currentEntityIri, iri(ns, currentPath), lit),
            ontologyLevel
          ) :: statements
        case StructType(_, _) =>
          foldingPhase match
            case BeginFoldingStruct =>
              val structIri = iri(ns, UUID.randomUUID.toString)
              statements = statement(
                triple(currentEntityIri, iri(ns, currentPath), structIri),
                ontologyLevel
              ) ::
                statement(triple(structIri, RDF.TYPE, NS.STRUCT), ontologyLevel)
                :: statements
              previousEntityIriStack.push(currentEntityIri)
              currentEntityIri = structIri
            case EndFoldingStruct =>
              currentEntityIri = previousEntityIriStack.pop()
            case _ =>
              ()
          end match
        case _ =>
          ()
      end match
    end internalEmitStatements

    unfoldTuple(tuple, schema, internalEmitStatements) match
      case Left(parsingError) =>
        Left[ManagementServiceError, List[Statement]](
          ManagementServiceError(
            s"The tuple is not conform to the schema: $parsingError"
          )
        )
      case Right(_) =>
        Right[ManagementServiceError, List[Statement]](statements)
    end match

  end emitStatementsForEntity

  def fetchEntityFieldsAndTypeName(
    logger: Logger[F],
    repository: KnowledgeGraph[F],
    instanceId: String
  ): F[(String, List[(String, String)])] = {
    fetchFieldsForInstance(logger, repository, instanceId)
      .map(lp => {
        val entityTypeNameOption = lp
          .filter(p => p(0) === "isClassifiedBy")
          .map(_(1))
          .headOption
          .map(value => iri(value).getLocalName)

        val entityTypeName = entityTypeNameOption.getOrElse("")
        val fields: List[(String, String)] =
          lp.filter(p => p(0) =!= "isClassifiedBy")
        (entityTypeName: String, fields)
      })
  }

  private def fetchFieldsForInstance(
    logger: Logger[F],
    repository: KnowledgeGraph[F],
    instanceId: String
  ): F[List[(String, String)]] =
    val query: String =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |SELECT ?field ?value WHERE {
         | BIND(iri("${ns.getName}$instanceId") as ?entity)
         |    ?entity ?field ?value .
         |    FILTER ( ?value not in ( ns:Entity, ns:Struct ))
         |  }
         |""".stripMargin

    val bindings = for {
      _ <- logger.trace(
        s"Evaluated query $query for retrieving instance $instanceId"
      )
      ibs <- repository.evaluateQuery(query)
    } yield ibs.toList

    val fieldsAndValues = bindings.map(bs =>
      bs.map(b =>
        val value = b.getBinding("value")
        val field: (String, String) = (
          iri(b.getBinding("field").getValue.stringValue()).getLocalName,
          value.getValue.stringValue()
        )
        field
      )
    )

    fieldsAndValues
  end fetchFieldsForInstance

  private def handlePrimitiveDataTypes(
    fieldName: String,
    dataType: DataType,
    fieldValue: Option[List[(String, String)]]
  ): F[Tuple] =
    val tuple = dataType match
      case StringType(mode, _) =>
        fieldValue match
          case Some(value) =>
            mode match
              case Required =>
                fieldName -> value(0)(1)
              case Repeated =>
                fieldName -> value.map(_(1)).reverse
              case Nullable =>
                if value(0)(1) === "null" then fieldName -> Option.empty[String]
                else fieldName -> Some(value(0)(1))
          case None =>
            fieldName -> List[String]()
      case DateType(mode, _) =>
        fieldValue match
          case Some(value) =>
            mode match
              case Required =>
                fieldName -> LocalDate.parse(value(0)(1))
              case Repeated =>
                fieldName -> value
                  .map(date => LocalDate.parse(date(1)))
                  .reverse
              case Nullable =>
                if value(0)(1) === "null" then
                  fieldName -> Option.empty[LocalDate]
                else fieldName -> Some(LocalDate.parse(value(0)(1)))
          case None =>
            fieldName -> List[LocalDate]()
      case JsonType(mode) =>
        fieldValue match
          case Some(value) =>
            mode match
              case Required =>
                fieldName -> io.circe.parser.parse(value(0)(1)).getOrElse("")
              case Repeated =>
                fieldName -> value
                  .map(date => io.circe.parser.parse(date(1)).getOrElse(""))
                  .reverse
              case Nullable =>
                if value(0)(1) === "null" then fieldName -> Option.empty[Json]
                else
                  fieldName -> Some(
                    io.circe.parser.parse(value(0)(1)).getOrElse("")
                  )
          case None =>
            fieldName -> List[Json]()
      case TimestampType(mode, _) =>
        fieldValue match
          case Some(value) =>
            mode match
              case Required =>
                fieldName -> ZonedDateTime.parse(value(0)(1))
              case Repeated =>
                fieldName -> value
                  .map(instant => ZonedDateTime.parse(instant(1)))
                  .reverse
              case Nullable =>
                if value(0)(1) === "null" then
                  fieldName -> Option.empty[ZonedDateTime]
                else fieldName -> Some(ZonedDateTime.parse(value(0)(1)))
          case None =>
            fieldName -> List[ZonedDateTime]()
      case DoubleType(mode, _) =>
        fieldValue match
          case Some(value) =>
            mode match
              case Required =>
                fieldName -> value(0)(1).toDouble
              case Repeated =>
                fieldName -> value.map(_(1).toDouble).reverse
              case Nullable =>
                if value(0)(1) === "null" then fieldName -> Option.empty[Double]
                else fieldName -> Some(value(0)(1).toDouble)
          case None =>
            fieldName -> List[Double]()
      case FloatType(mode, _) =>
        fieldValue match
          case Some(value) =>
            mode match
              case Required =>
                fieldName -> value(0)(1).toFloat
              case Repeated =>
                fieldName -> value.map(_(1).toFloat).reverse
              case Nullable =>
                if value(0)(1) === "null" then fieldName -> Option.empty[Float]
                else fieldName -> Some(value(0)(1).toFloat)
          case None =>
            fieldName -> List[Float]()
      case LongType(mode, _) =>
        fieldValue match
          case Some(value) =>
            mode match
              case Required =>
                fieldName -> value(0)(1).toLong
              case Repeated =>
                fieldName -> value.map(_(1).toLong).reverse
              case Nullable =>
                if value(0)(1) === "null" then fieldName -> Option.empty[Long]
                else fieldName -> Some(value(0)(1).toLong)
          case None =>
            fieldName -> List[Long]()
      case BooleanType(mode, _) =>
        fieldValue match
          case Some(value) =>
            mode match
              case Required =>
                fieldName -> value(0)(1).toBoolean
              case Repeated =>
                fieldName -> value.map(_(1).toBoolean).reverse
              case Nullable =>
                if value(0)(1) === "null" then
                  fieldName -> Option.empty[Boolean]
                else fieldName -> Some(value(0)(1).toBoolean)
          case None =>
            fieldName -> List[Boolean]()
      case IntType(mode, _) =>
        fieldValue match
          case Some(value) =>
            mode match
              case Required =>
                fieldName -> value(0)(1).toInt
              case Repeated =>
                fieldName -> value.map(_(1).toInt).reverse
              case Nullable =>
                if value(0)(1) === "null" then fieldName -> Option.empty[Int]
                else fieldName -> Some(value(0)(1).toInt)
          case None =>
            fieldName -> List[Int]()
      case _ => EmptyTuple
    tuple.pure[F]
  end handlePrimitiveDataTypes

  private def handleStructDataType(
    logger: Logger[F],
    repository: KnowledgeGraph[F],
    fieldName: String,
    dataType: StructType,
    maybeFieldValue: Option[List[(String, String)]]
  ): F[Tuple] =

    def createTupleForStructDataType(
      logger: Logger[F],
      dataType: StructType,
      fieldValue: (String, String)
    ): F[Tuple] =
      val nestedStructIRI: IRI = iri(fieldValue(1))
      val nestedStructFields: F[List[(String, String)]] =
        fetchFieldsForInstance(logger, repository, nestedStructIRI.getLocalName)
      nestedStructFields.flatMap(x =>
        fieldsToTuple(logger, repository, x, dataType)
      )
    end createTupleForStructDataType

    maybeFieldValue match
      case Some(fieldValue) =>
        dataType.mode match
          case Required =>
            val tuple =
              createTupleForStructDataType(logger, dataType, fieldValue.head)
            tuple.map(t => fieldName -> t)
          case Repeated =>
            val listOfFTuples: List[F[Tuple]] =
              fieldValue.map(createTupleForStructDataType(logger, dataType, _))
            val tuples: F[List[Tuple]] =
              Traverse[List].sequence(listOfFTuples)
            tuples.map(ts =>
              fieldName -> ts.reverse
            ) // TODO it could break with Virtuoso
          case Nullable =>
            if fieldValue(0)(1) === "null" then
              (fieldName -> Option.empty[Tuple]).pure[F]
            else
              val tuple =
                createTupleForStructDataType(logger, dataType, fieldValue.head)
              tuple.map(t => fieldName -> Some(t))
      case None =>
        (fieldName -> None).pure[F]
  end handleStructDataType

  def fieldsToTuple(
    logger: Logger[F],
    repository: KnowledgeGraph[F],
    fields: List[(String, String)],
    struct: StructType
  ): F[Tuple] =
    val groupedFields: Map[String, List[(String, String)]] =
      fields.groupBy(_(0))
    val tuples: List[F[Tuple]] = struct.records
      .map(record =>
        val (fieldName, dataType) = record
        val fieldValue = groupedFields.get(fieldName)
        dataType match
          case _: StringType =>
            handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
          case _: IntType =>
            handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
          case _: DateType =>
            handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
          case _: JsonType =>
            handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
          case _: TimestampType =>
            handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
          case _: DoubleType =>
            handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
          case _: FloatType =>
            handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
          case _: LongType =>
            handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
          case _: BooleanType =>
            handlePrimitiveDataTypes(fieldName, dataType, fieldValue)
          case struct: StructType =>
            handleStructDataType(
              logger,
              repository,
              fieldName,
              struct,
              fieldValue
            )
          case _ => EmptyTuple.pure[F]
      )
    Traverse[List].sequence(tuples).map(_.fold(EmptyTuple)(_ :* _))
  end fieldsToTuple

  @SuppressWarnings(
    Array("scalafix:DisableSyntax.asInstanceOf", "scalafix:DisableSyntax.==")
  )
  def fetchStatementsForInstance(
    repository: KnowledgeGraph[F],
    instanceId: String
  ): F[List[Statement]] =
    val query: String =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |SELECT ?s ?p ?o WHERE {
         | BIND(iri("${ns.getName}$instanceId") as ?s)
         |    ?s ?p ?o .
         |    FILTER(!CONTAINS(STR(?p), "${ns.getName}mappedTo#"))
         |  }
         |""".stripMargin
    val statements: F[List[Statement]] =
      repository
        .evaluateQuery(query)
        .flatMap(ibs =>
          Traverse[List].sequence(
            ibs
              .map(bs =>
                val sb = bs.getBinding("s")
                val pb = bs.getBinding("p")
                val ob = bs.getBinding("o")
                val s = iri(sb.getValue.stringValue)
                val p = iri(pb.getValue.stringValue)
                if Relationship.isRelationship(p.getLocalName) then
                  List.empty[Statement].pure[F]
                else if ob.getValue.isLiteral then
                  List(
                    statement(
                      triple(s, p, ob.getValue.asInstanceOf[Literal]),
                      L2
                    )
                  ).pure[F]
                else
                  if p == RDF.TYPE || p == NS.ISCLASSIFIEDBY then
                    List(
                      statement(triple(s, p, iri(ob.getValue.stringValue)), L2)
                    ).pure[F]
                  else
                    val stmt =
                      statement(triple(s, p, iri(ob.getValue.stringValue)), L2)
                    fetchStatementsForInstance(
                      repository,
                      iri(ob.getValue.stringValue).getLocalName
                    ).map(statements => stmt :: statements)
                  end if
                end if
              )
              .toList
          )
        )
        .map(_.flatten)
    statements
  end fetchStatementsForInstance

  @SuppressWarnings(Array("scalafix:DisableSyntax.=="))
  def updateInstanceNoCheck(
    logger: Logger[F],
    typeManagementService: TypeManagementService[F],
    instanceManagementService: InstanceManagementService[F],
    instanceId: String,
    values: Tuple
  ): F[Either[ManagementServiceError, String]] =

    given Logger[F] = logger

    val statementsToRemove: F[List[Statement]] = fetchStatementsForInstance(
      instanceManagementService.repository,
      instanceId
    )
    val res: F[Either[ManagementServiceError, String]] = (for {
      _ <- traceT(s"About to remove the instance $instanceId")
      stmts <- EitherT.liftF(statementsToRemove)
      _ <- traceT(
        s"Statements for removing the previous version \n${stmts.mkString("\n")}"
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
        cueValidate(entityType.schema, values)
          .leftMap(errors =>
            ManagementServiceError(s"Instance validation error" :: errors)
          )
          .pure[F]
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

    EitherT(instanceManagementService.exist(instanceId)).flatMapF { exist =>
      if exist then res
      else
        Left(
          ManagementServiceError(
            s"The instance with id $instanceId does not exist"
          )
        ).pure[F]
    }.value
  end updateInstanceNoCheck

  def getMappingsForEntityType(
    logger: Logger[F],
    typeManagementService: TypeManagementService[F],
    sourceEntityTypeName: String
  ): F[Either[ManagementServiceError, List[
    (String, EntityType, EntityType, Tuple, String)
  ]]] =
    val query =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT DISTINCT ?mr ?t ?m WHERE {
         |   ?mr ns:singletonPropertyOf ns:mappedTo .
         |   ns:$sourceEntityTypeName ?mr ?t .
         |   ?mr ns:mappedBy ?m .
         |}
         |""".stripMargin
    val res1 = Monad[F].flatMap(
      logger.trace(
        s"Retrieving the mappings for source type $sourceEntityTypeName with the query:\n$query"
      ) *>
        typeManagementService.repository.evaluateQuery(query)
    )(bit =>
      bit.toList
        .map(b =>
          val mappingName =
            iri(b.getBinding("mr").getValue.toString).getLocalName
          val mapperId = iri(b.getBinding("m").getValue.toString).getLocalName
          val targetEntityTypeName =
            iri(b.getBinding("t").getValue.toString).getLocalName
          (for {
            sourceEntityType <- EitherT(
              typeManagementService.read(sourceEntityTypeName)
            )
            targetEntityType <- EitherT(
              typeManagementService.read(targetEntityTypeName)
            )
            fields <- EitherT(
              fetchEntityFieldsAndTypeName(
                logger,
                typeManagementService.repository,
                mapperId
              ).map(t =>
                Right[ManagementServiceError, List[(String, String)]](t(1))
              )
            )
            mappedInstancesReferenceExpressions: Map[String, String] <- EitherT(
              Functor[F].map(
                getMappedInstancesReferenceExpressions(
                  logger,
                  typeManagementService,
                  mappingName,
                  sourceEntityTypeName
                )
              )(t => Right[ManagementServiceError, Map[String, String]](t))
            )
            listInstanceReferenceExpressions =
              mappedInstancesReferenceExpressions.toList
            tuple <- EitherT(
              fieldsToTuple(
                logger,
                typeManagementService.repository,
                fields ++ listInstanceReferenceExpressions,
                schemaToMapperSchema(targetEntityType.schema)
              ).map(Right[ManagementServiceError, Tuple])
            )
          } yield (
            mappingName,
            sourceEntityType,
            targetEntityType,
            tuple,
            mapperId
          )).value
        )
        .sequence
    )
    res1.map(_.sequence)
  end getMappingsForEntityType

  def getMappedInstancesReferenceExpressions(
    logger: Logger[F],
    typeManagementService: TypeManagementService[F],
    mappingName: String,
    sourceEntityTypeName: String
  ): F[Map[String, String]] =
    val query =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT DISTINCT ?name ?path WHERE {
         |   ?e1 ?map ?e2 .
         |   ?map ?pred ?mapId .
         |   ?mapId ?pred2 ?name .
         |   ?mapId ?pred3 ?path .
         |   FILTER(?e1 = <${ns.getName}${sourceEntityTypeName}>) .
         |   FILTER(?map = <${ns.getName}mappedTo#$mappingName>) .
         |   FILTER(?pred = <${ns.getName}withNamedInstanceReferenceExpression>) .
         |   FILTER(?pred2 = <${ns.getName}instanceReferenceName>) .
         |   FILTER(?pred3 = <${ns.getName}instanceReferenceExpression>) .
         |}
         |""".stripMargin

    typeManagementService.repository
      .evaluateQuery(query)
      .flatMap { queryResults =>
        queryResults.toList
          .traverse { bs =>
            val name = bs.getValue("name").stringValue()
            val path = bs.getValue("path").stringValue()
            (name, path).pure[F]
          }
          .map(_.toMap)
      }
  end getMappedInstancesReferenceExpressions

  def queryMappedInstances(
    logger: Logger[F],
    repository: KnowledgeGraph[F],
    sourceEntityTypeName: Option[String],
    mappingName: Option[String],
    targetEntityTypeName: Option[String]
  ): F[Either[ManagementServiceError, List[(String, String, String)]]] =
    val predicateSource = sourceEntityTypeName
      .map(value => s"?instance1 ns:isClassifiedBy ns:$value .")
      .getOrElse(s"?instance1 ns:isClassifiedBy ?entity3 .")
    val predicateTarget = targetEntityTypeName
      .map(value => s"?instance2 ns:isClassifiedBy ns:$value .")
      .getOrElse(s"?instance2 ns:isClassifiedBy ?entity4 .")

    val semiSourcePredicateMappedTo =
      sourceEntityTypeName.map(value => s"ns:$value").getOrElse("?entity1")
    val semiMappedToPredicate = mappingName
      .map(value => s"<${ns.getName}mappedTo#$value>")
      .getOrElse("?mapping")
    val semiTargetPredicateMappedTo = targetEntityTypeName
      .map(value => s"ns:$value .")
      .getOrElse("?entity2 .")

    val mappingPredicate: String = List(
      semiSourcePredicateMappedTo,
      semiMappedToPredicate,
      semiTargetPredicateMappedTo
    )
      .mkString(" ")

    val firstFilterMapping: String = mappingName
      .map(value => s"FILTER(?predicate = <${ns.getName}mappedTo#$value>) .")
      .getOrElse("")

    val secondFilterMapping: String = mappingName
      .map(value =>
        s"FILTER(STRSTARTS(STR(?mapping), '${ns.getName}mappedTo#$value')) ."
      )
      .getOrElse(
        s"FILTER(STRSTARTS(STR(?mapping), '${ns.getName}mappedTo#')) ."
      )

    val query =
      s"""
         |PREFIX ns: <https://w3id.org/agile-dm/ontology/>
         |SELECT ?instance1 ?predicate ?instance2 WHERE {
         |        ?instance1 ?predicate ?instance2 .
         |        $predicateSource
         |        $predicateTarget
         |        $mappingPredicate
         |        $firstFilterMapping
         |        $secondFilterMapping
         |}
         |""".stripMargin

    val res = logger.trace(
      s"Retrieving instances for the mapping ${mappingName.getOrElse(" ")} with the query:\n$query"
    ) *> repository.evaluateQuery(query)

    res.map(rows =>
      Right(
        rows.toList.map(row =>
          (
            (
              row.getValue("instance1").stringValue(),
              row.getValue("predicate").stringValue(),
              row.getValue("instance2").stringValue()
            )
          )
        )
      )
    )
  end queryMappedInstances

  def recursiveDelete(
    logger: Logger[F],
    repository: KnowledgeGraph[F],
    entityTypeName: String,
    typeManagementService: TypeManagementService[F],
    getAdditionalReferences: MappingKey => F[List[(String, String, String)]]
  ): F[Either[ManagementServiceError, Unit]] =
    val stack = scala.collection.mutable.Stack(entityTypeName)

    def loop(): F[Either[ManagementServiceError, Unit]] =
      if stack.isEmpty then Right(()).pure[F]
      else
        val currentType = stack.pop()
        getRelations(logger, repository, currentType, isGetParent = false)
          .flatMap { children =>
            children.foreach(stack.push)
            getMappingsForEntityType(logger, typeManagementService, currentType)
              .flatMap {
                case Right(mappings) =>
                  val filteredMappings = mappings.filter {
                    case (_, sourceEntityType, _, _, _) =>
                      sourceEntityType.name.equals(currentType)
                  }

                  if filteredMappings.isEmpty then Right(()).pure[F]
                  else
                    val (
                      mappingName,
                      sourceEntityType,
                      targetEntityType,
                      mapper,
                      mapperId
                    ) = filteredMappings.head
                    val firstMappingDefinition = MappingDefinition(
                      MappingKey(
                        mappingName,
                        sourceEntityType.name,
                        targetEntityType.name
                      ),
                      mapper
                    )

                    deleteMapping(
                      logger,
                      repository,
                      typeManagementService,
                      firstMappingDefinition,
                      mapperId,
                      getAdditionalReferences
                    ).map(Right(_))

                case Left(error) => Left(error).pure[F]
              }
              .flatMap {
                case Right(_)    => loop()
                case Left(error) => Left(error).pure[F]
              }
          }
          .handleErrorWith { error =>
            Left(
              ManagementServiceError(
                s"There was an error during the deletion of the mapping: $error"
              )
            ).pure[F]
          }
    loop()
  end recursiveDelete

  def deleteMapping(
    logger: Logger[F],
    repository: KnowledgeGraph[F],
    typeManagementService: TypeManagementService[F],
    mappingDefinition: MappingDefinition,
    mapperId: String,
    getAdditionalReferences: MappingKey => F[List[(String, String, String)]]
  ): F[Either[ManagementServiceError, Unit]] =
    val key = mappingDefinition.mappingKey
    val mapper = mappingDefinition.mapper
    val sourceEntityTypeIri = iri(ns, key.sourceEntityTypeName)
    val targetEntityTypeIri = iri(ns, key.targetEntityTypeName)
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

    val initialStatementsL2 = List(
      statement(mappedToTriple1, L2),
      statement(mappedToTriple2, L2),
      statement(mappedToTriple3, L2)
    )

    val initialStatementsL3 =
      List(statement(mappedToTriple2, L2), statement(mappedToTriple3, L2))

    (for {
      ttype <- EitherT(typeManagementService.read(key.targetEntityTypeName))
      additionalReferencesWithIds <- EitherT.liftF(
        getAdditionalReferences(mappingDefinition.mappingKey)
      )
      additionalReferenceStatements =
        additionalReferencesWithIds.flatMap(reference =>
          val namedIri = iri(ns, reference._1)
          val namedInstanceTriple = triple(
            iri(
              mappedTo.getNamespace,
              s"${mappedTo: String}#${key.mappingName}"
            ),
            NS.WITHNAMEDINSTANCEREFERENCEEXPRESSION,
            namedIri
          )
          val referenceName =
            triple(namedIri, NS.INSTANCEREFERENCENAME, literal(reference._2))
          val referenceExpression =
            triple(
              namedIri,
              NS.INSTANCEREFERENCEEXPRESSION,
              literal(reference._3)
            )
          List(
            statement(namedInstanceTriple, L2),
            statement(referenceName, L2),
            statement(referenceExpression, L2)
          )
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
            List.empty[Statement],
            initialStatementsL2 ::: initialStatementsL3 ::: stmts ::: additionalReferenceStatements
          )
          .map(Right[ManagementServiceError, Unit])
      )
    } yield res).value
  end deleteMapping

  def getMappingsForEntity(
    logger: Logger[F],
    typeManagementService: TypeManagementService[F],
    instanceManagementService: InstanceManagementService[F],
    sourceEntityId: String
  ): F[Either[ManagementServiceError, List[
    (EntityType, Entity, EntityType, Entity, Tuple, String)
  ]]] =
    val query =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT distinct ?mr ?t ?m WHERE {
         |   ?mr ns:singletonPropertyOf ns:mappedTo .
         |   ns:$sourceEntityId ?mr ?t .
         |   ?mr ns:mappedBy ?m .
         |}
         |""".stripMargin
    val res1 =
      logger.trace(
        s"Retrieving the mappings for source id $sourceEntityId with the query:\n$query"
      ) *>
        instanceManagementService.repository
          .evaluateQuery(query)
          .flatMap(bit =>
            bit.toList
              .map(b =>
                val mapperId =
                  iri(b.getBinding("m").getValue.toString).getLocalName
                val mappingRelationship = iri(
                  b.getBinding("mr").getValue.toString
                ).toString.replaceFirst(ns.getName, "")
                val targetEntityId =
                  iri(b.getBinding("t").getValue.toString).getLocalName
                (for {
                  sourceEntity <- EitherT(
                    instanceManagementService.read(sourceEntityId)
                  )
                  sourceEntityType <- EitherT(
                    typeManagementService.read(sourceEntity.entityTypeName)
                  )
                  targetEntity <- EitherT(
                    instanceManagementService.read(targetEntityId)
                  )
                  targetEntityType <- EitherT(
                    typeManagementService.read(targetEntity.entityTypeName)
                  )
                  fields <- EitherT(
                    fetchEntityFieldsAndTypeName(
                      logger,
                      instanceManagementService.repository,
                      mapperId
                    ).map(t =>
                      Right[ManagementServiceError, List[(String, String)]](
                        t(1)
                      )
                    )
                  )
                  tuple <- EitherT(
                    fieldsToTuple(
                      logger,
                      instanceManagementService.repository,
                      fields,
                      schemaToMapperSchema(targetEntityType.schema)
                    ).map(Right[ManagementServiceError, Tuple])
                  )
                } yield (
                  sourceEntityType,
                  sourceEntity,
                  targetEntityType,
                  targetEntity,
                  tuple,
                  mappingRelationship
                )).value
              )
              .sequence
          )
    res1.map(_.sequence)
  end getMappingsForEntity

  def getRelations(
    logger: Logger[F],
    repository: KnowledgeGraph[F],
    sourceEntityTypeName: String,
    isGetParent: Boolean
  ): F[List[String]] =
    val nsName = ns.getName
    val (relationship, direction) =
      if isGetParent then
        (
          "?mr ns:singletonPropertyOf ns:mappedTo",
          s"?t ?mr ns:$sourceEntityTypeName"
        )
      else
        (
          s"ns:$sourceEntityTypeName ?mr ?t",
          "?mr ns:singletonPropertyOf ns:mappedTo"
        )

    val query =
      s"""
         |PREFIX ns:   <$nsName>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT distinct ?mr ?t ?m WHERE {
         |   $relationship .
         |   $direction .
         |   ?mr ns:mappedBy ?m .
         |}
         |""".stripMargin

    logger.trace(
      s"Executing getRelations for $sourceEntityTypeName with query:\n$query"
    ) >>
      repository.evaluateQuery(query).flatMap { iterator =>
        @tailrec
        def loop(acc: List[String]): List[String] =
          if !iterator.hasNext then acc
          else
            val bindingSet = iterator.next()
            val tValueOriginal = bindingSet.getValue("t").stringValue()
            val tValue =
              if tValueOriginal.startsWith(nsName) then
                tValueOriginal.stripPrefix(nsName)
              else tValueOriginal

            loop(tValue :: acc)

        loop(Nil).reverse.pure[F]
      }

  def getRoots(
    logger: Logger[F],
    repository: KnowledgeGraph[F],
    entityTypeName: String
  ): F[List[String]] =
    def loop(pending: Set[String], roots: Set[String]): F[Set[String]] =
      if pending.isEmpty then roots.pure[F]
      else
        val next = pending.head
        getRelations(logger, repository, next, true).flatMap { parents =>
          if parents.isEmpty then loop(pending - next, roots + next)
          else loop(pending - next ++ parents, roots)
        }

    getRelations(logger, repository, entityTypeName, true).flatMap {
      initialParents =>
        if !initialParents.isEmpty then
          loop(initialParents.toSet, Set.empty).map(_.toList)
        else List(entityTypeName).pure[F]
    }
  end getRoots

  def detectCycles(
    logger: Logger[F],
    repository: KnowledgeGraph[F],
    sourceEntityTypeName: String,
    targetEntityTypeName: String
  ): F[Either[ManagementServiceError, List[String]]] =
    def loop(
      pendingTypes: mutable.Stack[String],
      visited: Set[String],
      accumulator: List[String]
    ): F[Either[ManagementServiceError, List[String]]] =
      if pendingTypes.isEmpty then Right(accumulator).pure[F]
      else
        val currentType = pendingTypes.pop()
        if visited.contains(currentType) then
          Left(
            ManagementServiceError(
              s"Cycle detected in the hierarchy when processing '$currentType'"
            )
          ).pure[F]
        else
          getRelations(logger, repository, currentType, true).flatMap { roots =>
            val newVisited = visited + currentType
            val commonRoots = roots.toSet.intersect(newVisited)
            if commonRoots.nonEmpty then
              Left(
                ManagementServiceError(
                  s"Cycle detected in the hierarchy when processing one of the roots of '$currentType'"
                )
              ).pure[F]
            else if roots.contains(currentType) then
              loop(pendingTypes, newVisited, currentType :: accumulator)
            else
              roots.filterNot(newVisited.contains).foreach(pendingTypes.push)
              loop(pendingTypes, newVisited, accumulator)
          }

    loop(mutable.Stack(sourceEntityTypeName), Set(targetEntityTypeName), Nil)
  end detectCycles

  def checkTraitForEntityType(
    logger: Logger[F],
    repository: KnowledgeGraph[F],
    entityTypeName: String,
    traitName: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val query =
      s"""
         |PREFIX ns: <https://w3id.org/agile-dm/ontology/>
         |SELECT (COUNT(*) as ?count) WHERE {
         |        ns:$entityTypeName ns:hasTrait ?trait1 .
         |        ?trait1 rdfs:subClassOf* ns:$traitName .
         |}
         |""".stripMargin
    val res = logger.trace(
      s"Checking if the type $entityTypeName is related to the trait $traitName with the query:\n$query"
    ) *> repository.evaluateQuery(query)
    res.map(res =>
      val count = res.toList.headOption
        .flatMap(row => Option(row.getValue("count")))
        .flatMap(_.stringValue().toIntOption)
        .getOrElse(0)
      if count > 0 then Right[ManagementServiceError, Boolean](true)
      else Right[ManagementServiceError, Boolean](false)
      end if
    )
  end checkTraitForEntityType

end InstanceManagementServiceInterpreterCommonFunctions
