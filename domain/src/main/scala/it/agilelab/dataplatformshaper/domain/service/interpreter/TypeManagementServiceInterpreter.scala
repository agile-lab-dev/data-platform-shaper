package it.agilelab.dataplatformshaper.domain.service.interpreter

import cats.*
import cats.data.*
import cats.effect.*
import cats.syntax.*
import cats.syntax.all.*
import io.chrisdavenport.mules.Cache
import it.agilelab.dataplatformshaper.domain.common.EitherTLogging.traceT
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.*
import it.agilelab.dataplatformshaper.domain.model.NS.*
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.service.{
  ManagementServiceError,
  TraitManagementService,
  TypeManagementService
}
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, literal, triple}
import org.eclipse.rdf4j.model.vocabulary.{OWL, RDF, RDFS}
import org.eclipse.rdf4j.model.{IRI, Statement}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.util.UUID
import scala.language.{implicitConversions, postfixOps}

class TypeManagementServiceInterpreter[F[_]: Sync](
    traitManagementService: TraitManagementService[F]
)(using cache: Cache[F, String, EntityType])
    extends TypeManagementService[F]:

  val repository: KnowledgeGraph[F] = traitManagementService.repository

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.=="
    )
  )
  implicit val dataTypeEq: Eq[DataType] = Eq.instance[DataType] { (x, y) =>
    (x, y) match {
      case (a: IntType, b: IntType)             => a.mode == b.mode
      case (a: LongType, b: LongType)           => a.mode == b.mode
      case (a: FloatType, b: FloatType)         => a.mode == b.mode
      case (a: DoubleType, b: DoubleType)       => a.mode == b.mode
      case (a: SqlDecimal, b: SqlDecimal)       => a.mode == b.mode
      case (a: BooleanType, b: BooleanType)     => a.mode == b.mode
      case (a: StringType, b: StringType)       => a.mode == b.mode
      case (a: TimestampType, b: TimestampType) => a.mode == b.mode
      case (a: DateType, b: DateType)           => a.mode == b.mode
      case (a: JsonType, b: JsonType)           => a.mode == b.mode
      case (a: StructType, b: StructType)       => structTypeEq.eqv(a, b)
      case _                                    => false
    }
  }

  implicit val structTypeEq: Eq[StructType] = Eq.instance[StructType] {
    (x, y) =>
      val c1: Map[String, DataType] = x.records.toMap
      val c2: Map[String, DataType] = y.records.toMap

      c1.keys.forall { key =>
        (c1.get(key), c2.get(key)) match {
          case (Some(a), Some(b)) => dataTypeEq.eqv(a, b)
          case _                  => false
        }
      }
  }

  extension (entityType: EntityType)
    def inheritsFrom(fatherName: String)(using
        tms: TypeManagementService[F]
    ): F[Either[ManagementServiceError, EntityType]] =
      entityType.father.fold(
        tms
          .read(fatherName)
          .map(
            _.map(fatherEntityType =>
              entityType.copy(father = Some(fatherEntityType))
            )
          )
      )(_ =>
        summon[Applicative[F]].pure(
          Right[ManagementServiceError, EntityType](entityType)
        )
      )
    end inheritsFrom
  end extension

  given logger: Logger[F] = Slf4jLogger.getLogger[F]

  private def constraintsToStatement(
      entity: IRI,
      constraints: Option[String]
  ): Statement =
    statement(
      triple(entity, NS.CONSTRAINTS, literal(constraints.getOrElse("null"))),
      L1
    )
  end constraintsToStatement

  private def modeToStatement(entity: IRI, mode: Mode): Statement =
    mode match
      case Nullable =>
        statement(triple(entity, NS.MODE, NS.NULLABLE), L1)
      case Repeated =>
        statement(triple(entity, NS.MODE, NS.REPEATED), L1)
      case Required =>
        statement(triple(entity, NS.MODE, NS.REQUIRED), L1)
    end match
  end modeToStatement

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.defaultArgs"
    )
  )
  private def stringToDataType(
      stringType: String,
      stringMode: String,
      constraints: String,
      records: Option[List[(String, DataType)]] = None
  ): DataType =
    stringType match
      case "StringAttributeType" =>
        StringType(
          modeStringToMode(stringMode),
          constraintsStringToConstraints(constraints)
        )
      case "IntAttributeType" =>
        IntType(
          modeStringToMode(stringMode),
          constraintsStringToConstraints(constraints)
        )
      case "DateAttributeType" =>
        DateType(
          modeStringToMode(stringMode),
          constraintsStringToConstraints(constraints)
        )
      case "JsonAttributeType" =>
        JsonType(modeStringToMode(stringMode))
      case "TimestampAttributeType" =>
        TimestampType(
          modeStringToMode(stringMode),
          constraintsStringToConstraints(constraints)
        )
      case "DoubleAttributeType" =>
        DoubleType(
          modeStringToMode(stringMode),
          constraintsStringToConstraints(constraints)
        )
      case "FloatAttributeType" =>
        FloatType(
          modeStringToMode(stringMode),
          constraintsStringToConstraints(constraints)
        )
      case "LongAttributeType" =>
        LongType(
          modeStringToMode(stringMode),
          constraintsStringToConstraints(constraints)
        )
      case "BooleanAttributeType" =>
        BooleanType(
          modeStringToMode(stringMode),
          constraintsStringToConstraints(constraints)
        )
      case _ =>
        StructType(records.getOrElse(List.empty), modeStringToMode(stringMode))
  end stringToDataType

  private def modeStringToMode(modeString: String): Mode =
    modeString match
      case "Required" => Required
      case "Repeated" => Repeated
      case "Nullable" => Nullable
    end match
  end modeStringToMode

  private def constraintsStringToConstraints(
      constraintsString: String
  ): Option[String] =
    if constraintsString === "null" then None else Some(constraintsString)

  private def isStructType(fieldType: String): Boolean =
    fieldType match
      case "StringAttributeType" | "IntAttributeType" => false
      case _                                          => true
  end isStructType

  private def emitStatements(
      fatherEntity: IRI,
      dataType: DataType,
      childEntity: IRI,
      currentPath: String
  ): List[Statement] =
    dataType match
      case StructType(records, mode) =>
        val structTypeInstance = iri(ns, UUID.randomUUID().toString)
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L1),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L1),
          statement(triple(childEntity, RDFS.RANGE, structTypeInstance), L1),
          statement(triple(structTypeInstance, RDF.TYPE, NS.STRUCTTYPE), L1),
          modeToStatement(childEntity, mode),
          constraintsToStatement(childEntity, None)
        ) ++ records.flatMap(record =>
          emitStatements(
            structTypeInstance,
            record(1),
            iri(ns, s"$currentPath/${record(0)}"),
            s"$currentPath/${record(0)}"
          )
        )
      case StringType(mode, constraints) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L1),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L1),
          statement(
            triple(childEntity, RDFS.RANGE, NS.STRINGATTRIBUTETYPE),
            L1
          ),
          modeToStatement(childEntity, mode),
          constraintsToStatement(childEntity, constraints)
        )
      case DateType(mode, constraints) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L1),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L1),
          statement(
            triple(childEntity, RDFS.RANGE, NS.DATEATTRIBUTETYPE),
            L1
          ),
          modeToStatement(childEntity, mode),
          constraintsToStatement(childEntity, constraints)
        )
      case JsonType(mode) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L1),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L1),
          statement(
            triple(childEntity, RDFS.RANGE, NS.JSONATTRIBUTETYPE),
            L1
          ),
          modeToStatement(childEntity, mode),
          constraintsToStatement(childEntity, None)
        )
      case TimestampType(mode, constraints) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L1),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L1),
          statement(
            triple(childEntity, RDFS.RANGE, NS.TIMESTAMPATTRIBUTETYPE),
            L1
          ),
          modeToStatement(childEntity, mode),
          constraintsToStatement(childEntity, constraints)
        )
      case DoubleType(mode, constraints) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L1),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L1),
          statement(
            triple(childEntity, RDFS.RANGE, NS.DOUBLEATTRIBUTETYPE),
            L1
          ),
          modeToStatement(childEntity, mode),
          constraintsToStatement(childEntity, constraints)
        )
      case FloatType(mode, constraints) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L1),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L1),
          statement(
            triple(childEntity, RDFS.RANGE, NS.FLOATATTRIBUTETYPE),
            L1
          ),
          modeToStatement(childEntity, mode),
          constraintsToStatement(childEntity, constraints)
        )
      case LongType(mode, constraints) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L1),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L1),
          statement(triple(childEntity, RDFS.RANGE, NS.LONGATTRIBUTETYPE), L1),
          modeToStatement(childEntity, mode),
          constraintsToStatement(childEntity, constraints)
        )
      case BooleanType(mode, constraints) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L1),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L1),
          statement(
            triple(childEntity, RDFS.RANGE, NS.BOOLEANATTRIBUTETYPE),
            L1
          ),
          modeToStatement(childEntity, mode),
          constraintsToStatement(childEntity, constraints)
        )
      case IntType(mode, constraints) =>
        List(
          statement(triple(fatherEntity, NS.HASATTRIBUTETYPE, childEntity), L1),
          statement(triple(childEntity, RDFS.DOMAIN, fatherEntity), L1),
          statement(triple(childEntity, RDFS.RANGE, NS.INTATTRIBUTETYPE), L1),
          modeToStatement(childEntity, mode),
          constraintsToStatement(childEntity, constraints)
        )
      case _ =>
        List.empty[Statement]
    end match
  end emitStatements

  private def emitStatementsFromSchema(
      instanceType: IRI,
      schema: Schema
  ): List[Statement] =
    schema.records.flatMap(record =>
      emitStatements(
        instanceType,
        record(1),
        iri(ns, s"${instanceType.getLocalName}/${record(0)}"),
        s"${instanceType.getLocalName}/${record(0)}"
      )
    )
  end emitStatementsFromSchema

  private def queryForType(entityType: IRI): String =
    s"""
       |PREFIX ns:  <${ns.getName}>
       |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
       |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
       |SELECT ?field ?type ?mode ?constraints WHERE {
       |    BIND(iri("${entityType.stringValue()}") as ?entityType)
       |    ?field rdfs:domain ?entityType .
       |    ?field rdfs:range  ?type .
       |    ?field ns:mode ?mode .
       |    ?field ns:constraints ?constraints .
       |  }
       |""".stripMargin
  end queryForType

  private def getStructTypeRecords(
      structIri: IRI
  ): F[List[(String, DataType)]] =
    val query = queryForType(structIri)
    repository
      .evaluateQuery(query)
      .map(ibs =>
        val records: Iterator[F[(String, DataType)]] = ibs.map(bs =>
          val fieldName =
            iri(bs.getBinding("field").getValue.stringValue()).getLocalName
          val fieldType =
            iri(bs.getBinding("type").getValue.stringValue()).getLocalName
          val fieldMode =
            iri(bs.getBinding("mode").getValue.stringValue()).getLocalName
          val fieldConstraints =
            bs.getBinding("constraints").getValue.stringValue()
          if !isStructType(fieldType) then
            summon[Applicative[F]]
              .pure(
                fieldName -> stringToDataType(
                  fieldType,
                  fieldMode,
                  fieldConstraints,
                  None
                )
              )
          else
            getStructTypeRecords(
              iri(bs.getBinding("type").getValue.stringValue())
            )
              .map(records =>
                fieldName -> stringToDataType(
                  fieldType,
                  fieldMode,
                  fieldConstraints,
                  Some(records)
                )
              )
          end if
        )
        Traverse[List].sequence(records.toList)
      )
      .flatten
  end getStructTypeRecords

  private def getFatherFromEntityType(
      entityTypeName: String
  ): F[Either[ManagementServiceError, Option[EntityType]]] =
    val entityTypeIri = iri(ns, entityTypeName)
    val query: String =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT ?father WHERE {
         | BIND(iri("${entityTypeIri.stringValue()}") as ?entityType)
         |    ?entityType ns:inheritsFrom ?father .
         |  }
         |""".stripMargin
    (for {
      fatherName <- repository
        .evaluateQuery(query)
        .map(
          _.map(bs =>
            bs.getBinding("father").getValue match
              case iri: IRI => iri.getLocalName
          ).toList.headOption
        )
    } yield fatherName match
      case None =>
        summon[Applicative[F]].pure(
          Right[ManagementServiceError, Option[EntityType]](
            None: Option[EntityType]
          )
        ): F[Either[ManagementServiceError, Option[EntityType]]]
      case Some(name) =>
        read(name).map(_.map(et => Some(et)))
    ).flatten
  end getFatherFromEntityType

  private def getTraitsFromEntityType(
      entityTypeName: String
  ): F[Either[ManagementServiceError, Set[String]]] =
    val entityTypeIri = iri(ns, entityTypeName)
    val query: String =
      s"""
         |PREFIX ns:   <${ns.getName}>
         |PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         |PREFIX xsd:  <http://www.w3.org/2001/XMLSchema#>
         |SELECT ?trait WHERE {
         | BIND(iri("${entityTypeIri.stringValue()}") as ?entityType)
         |    ?entityType ns:hasTrait ?trait .
         |  }
         |""".stripMargin
    repository
      .evaluateQuery(query)
      .map(
        _.map(bs =>
          bs.getBinding("trait").getValue match
            case iri: IRI => iri.getLocalName
        ).toSet
      )
      .map(Right[ManagementServiceError, Set[String]])
  end getTraitsFromEntityType

  private def getSchemaFromEntityType(entityTypeName: String): F[Schema] =
    val entityTypeIri = iri(ns, entityTypeName)
    val query = queryForType(entityTypeIri)
    repository
      .evaluateQuery(query)
      .map(ibs =>
        Traverse[List].sequence(
          ibs
            .map(bs =>
              val fieldName =
                iri(bs.getBinding("field").getValue.stringValue()).getLocalName
              val fieldType =
                iri(bs.getBinding("type").getValue.stringValue()).getLocalName
              val fieldMode =
                iri(bs.getBinding("mode").getValue.stringValue()).getLocalName
              val fieldConstraints =
                bs.getBinding("constraints").getValue.stringValue()
              if !isStructType(fieldType) then
                summon[Applicative[F]].pure(
                  fieldName -> stringToDataType(
                    fieldType,
                    fieldMode,
                    fieldConstraints,
                    None
                  )
                )
              else
                getStructTypeRecords(
                  iri(bs.getBinding("type").getValue.stringValue())
                )
                  .map(records =>
                    fieldName -> stringToDataType(
                      fieldType,
                      fieldMode,
                      fieldConstraints,
                      Some(records)
                    )
                  )
              end if
            )
            .toList
        )
      )
      .flatMap(_.map(StructType(_, Required)))
  end getSchemaFromEntityType

  private def createOrDelete(
      entityType: EntityType,
      isCreation: Boolean
  ): F[Either[ManagementServiceError, Unit]] =
    val instanceType = iri(ns, entityType.name)
    val statementsForInheritance
        : F[Either[ManagementServiceError, List[Statement]]] =
      entityType.father.fold(
        summon[Applicative[F]].pure(
          Right[ManagementServiceError, List[Statement]](List.empty[Statement])
        )
      )(entityType =>
        exist(entityType.name).map(
          _.map(exist =>
            if exist then
              List(
                statement(
                  triple(instanceType, INHERITSFROM, iri(ns, entityType.name)),
                  L1
                )
              )
            else List.empty[Statement]
          )
        )
      )

    val traitsStatements = entityType.traits
      .map(`trait` =>
        statement(
          triple(instanceType, NS.HASTRAIT, iri(ns, `trait`: String)),
          L1
        )
      )
      .toList

    val typeInstanceStatements = List(
      statement(triple(instanceType, RDF.TYPE, OWL.NAMEDINDIVIDUAL), L1),
      statement(triple(instanceType, RDF.TYPE, ENTITYTYPE), L1),
      statement(triple(instanceType, TYPENAME, literal(entityType.name)), L1)
    )

    val attributeStatements =
      emitStatementsFromSchema(instanceType, entityType.baseSchema)
    (for {
      exist <- EitherT(exist(entityType.name))
      traitExistence <- EitherT(traitManagementService.exist(entityType.traits))
      _ <- {
        val nonExistentTraits =
          traitExistence.filter(!_._2).map(_._1).toList
        if (nonExistentTraits.isEmpty)
          EitherT.rightT[F, ManagementServiceError](())
        else
          EitherT.leftT[F, Unit](
            ManagementServiceError(
              s"The trait ${nonExistentTraits.head} does not exist"
            )
          )
      }
      stmts <- EitherT(statementsForInheritance)
      _ <- EitherT[F, ManagementServiceError, Unit](
        summon[Applicative[F]].pure(
          cueValidateModel(entityType.schema).leftMap(errors =>
            ManagementServiceError(s"Invalid constraints" :: errors)
          )
        )
      )
      _ <- EitherT {
        if isCreation then
          if !exist then
            summon[Functor[F]].map(
              repository.removeAndInsertStatements(
                stmts ++ traitsStatements ++ typeInstanceStatements ++ attributeStatements
              )
            )(Right[ManagementServiceError, Unit])
          else
            summon[Applicative[F]]
              .pure(
                Left[ManagementServiceError, Unit](
                  ManagementServiceError(
                    s"The EntityType ${entityType.name} has been already defined"
                  )
                )
              )
        else
          summon[Functor[F]].map(
            repository.removeAndInsertStatements(
              List.empty[Statement],
              stmts ++ traitsStatements ++ typeInstanceStatements ++ attributeStatements
            )
          )(Right[ManagementServiceError, Unit])
      }
      _ <- traceT(s"Instance type ${if (isCreation) "created" else "deleted"}")
    } yield ()).value
  end createOrDelete

  override def create(
      entityType: EntityType,
      inheritsFrom: String
  ): F[Either[ManagementServiceError, Unit]] =
    given TypeManagementService[F] = this
    (for {
      enrichedEntityType <- EitherT(entityType.inheritsFrom(inheritsFrom))
      _ <- EitherT(createOrDelete(enrichedEntityType, true))
    } yield ()).value
  end create

  override def create(
      entityType: EntityType
  ): F[Either[ManagementServiceError, Unit]] =
    createOrDelete(entityType, true)
  end create

  override def read(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, EntityType]] =
    (for {
      _ <- traceT(s"read $instanceTypeName")
      exist <- EitherT(exist(instanceTypeName))
      _ <- traceT(s"read $instanceTypeName: existence check true/false: $exist")
      definitionWithExistCheck <- EitherT {
        if exist
        then
          (for {
            retrieved <- EitherT(
              summon[Functor[F]].map(cache.lookup(instanceTypeName))(
                Right[ManagementServiceError, Option[EntityType]]
              )
            )
            _ <- traceT(
              s"read $instanceTypeName: check in the cache $retrieved"
            )
            definitionWithCacheLookup <- EitherT(
              retrieved
                .fold(
                  (for {
                    _ <- traceT(
                      s"read $instanceTypeName: not found in the cache"
                    )
                    traits <- EitherT(getTraitsFromEntityType(instanceTypeName))
                    _ <- traceT(
                      s"read $instanceTypeName: retrieved traits $traits"
                    )
                    father <- EitherT(getFatherFromEntityType(instanceTypeName))
                    _ <- traceT(
                      s"read $instanceTypeName: retrieved father $father if any"
                    )
                    entityType <- EitherT(
                      getSchemaFromEntityType(instanceTypeName).map(schema =>
                        Right[ManagementServiceError, EntityType](
                          EntityType(
                            instanceTypeName,
                            traits,
                            schema,
                            father
                          )
                        )
                      )
                    )
                    _ <- traceT(
                      s"read $instanceTypeName: candidate $entityType"
                    )
                    et <- EitherT(
                      cache
                        .insert(instanceTypeName, entityType)
                        .map(_ =>
                          Right[ManagementServiceError, EntityType](entityType)
                        )
                    )
                  } yield et).value
                )(et =>
                  logger.trace(s"found in the cache") *> summon[Applicative[F]]
                    .pure(Right[ManagementServiceError, EntityType](et))
                )
            )
          } yield definitionWithCacheLookup).value
        else
          summon[Applicative[F]].pure(
            Left[ManagementServiceError, EntityType](
              ManagementServiceError(
                s"The EntityType $instanceTypeName does not exist"
              )
            )
          )
        end if
      }
    } yield definitionWithExistCheck).value
  end read

  private def hasInstances(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, Boolean]] = {
    val instanceCheckQuery =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |SELECT (COUNT(?instance) as ?instanceCount) WHERE {
         |   ?type ns:typeName "$instanceTypeName"^^xsd:string .
         |   ?subtype rdf:type ns:EntityType .
         |   ?subtype ns:inheritsFrom* ?type .
         |   ?instance ns:isClassifiedBy ?subtype .
         |}
         |""".stripMargin

    val instanceCheckResult = repository.evaluateQuery(instanceCheckQuery)
    summon[Functor[F]].map(instanceCheckResult) { resultSet =>
      val resultList = resultSet.toList
      Right(
        resultList.nonEmpty && resultList.head
          .getValue("instanceCount")
          .stringValue()
          .toInt > 0
      )
    }
  }

  private def isFather(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, Boolean]] = {
    val isFatherQuery: String =
      s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
         |SELECT (COUNT(?subtype) as ?subTypeCount) WHERE {
         |   ?type ns:typeName "$instanceTypeName"^^xsd:string .
         |   ?subtype rdf:type ns:EntityType .
         |   ?subtype ns:inheritsFrom* ?type .
         |}
         |""".stripMargin

    val isFatherCheckResult = repository.evaluateQuery(isFatherQuery)
    summon[Functor[F]].map(isFatherCheckResult) { resultSet =>
      val resultList = resultSet.toList
      Right(
        resultList.nonEmpty && resultList.head
          .getValue("subTypeCount")
          .stringValue()
          .toInt > 1
      )
    }
  }

  override def delete(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, Unit]] = {
    val existenceCheck = exist(instanceTypeName)
    val testEntity = read(instanceTypeName)
    summon[Monad[F]].flatMap(existenceCheck) {
      case Right(true) =>
        val instanceCheck = hasInstances(instanceTypeName)
        summon[Monad[F]].flatMap(instanceCheck) {
          case Right(true) =>
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](
                ManagementServiceError(
                  s"The EntityType $instanceTypeName cannot be deleted because there are related instances"
                )
              )
            )

          case Right(false) =>
            val isFatherCheck = isFather(instanceTypeName)
            summon[Monad[F]].flatMap(isFatherCheck) {
              case Right(true) =>
                summon[Applicative[F]].pure(
                  Left[ManagementServiceError, Unit](
                    ManagementServiceError(
                      s"The EntityType $instanceTypeName cannot be deleted because is inherited by other EntityType"
                    )
                  )
                )

              case Right(false) =>
                summon[Monad[F]].flatMap(testEntity) {
                  case Right(entity) =>
                    val schemaF = getSchemaFromEntityType(instanceTypeName)
                    cache.delete(instanceTypeName) *>
                      summon[Monad[F]].flatMap(schemaF) { schema =>
                        val entityType =
                          EntityType(instanceTypeName, entity.traits, schema)
                        createOrDelete(entityType, isCreation = false)
                      }
                  case Left(error) =>
                    summon[Applicative[F]].pure(Left(error))
                }

              case Left(error) =>
                summon[Applicative[F]].pure(
                  Left[ManagementServiceError, Unit](error)
                )
            }

          case Left(error) =>
            summon[Applicative[F]].pure(
              Left[ManagementServiceError, Unit](error)
            )
        }

      case Right(false) =>
        summon[Applicative[F]].pure(
          Left[ManagementServiceError, Unit](
            ManagementServiceError(
              s"The EntityType $instanceTypeName does not exist"
            )
          )
        )

      case Left(error) =>
        summon[Applicative[F]].pure(Left[ManagementServiceError, Unit](error))
    }
  }

  override def updateConstraints(
      entityTypeRequest: EntityType
  ): F[Either[ManagementServiceError, Unit]] =
    val instanceType = iri(ns, entityTypeRequest.name)
    (for {
      entityTypeResult <- EitherT(read(entityTypeRequest.name))
      _ <- EitherT[F, ManagementServiceError, Unit](
        summon[Applicative[F]].pure(
          cueValidateModel(entityTypeRequest.schema).leftMap(errors =>
            ManagementServiceError(s"Invalid constraints" :: errors)
          )
        )
      )
      _ <-
        EitherT(
          summon[Monad[F]].pure(
            if (entityTypeResult.schema === entityTypeRequest.schema) {
              Right[ManagementServiceError, Unit](())
            } else {
              Left[ManagementServiceError, Unit](
                ManagementServiceError(
                  "Schemas did not match during update"
                )
              )
            }
          )
        )
      previousEntityType <- EitherT(read(entityTypeRequest.name))
      previousEntityTypeIRI <- EitherT(
        summon[Functor[F]].pure(
          Right[ManagementServiceError, IRI](
            iri(ns, previousEntityType.name)
          )
        )
      )
      allPreviousStatements <- EitherT(
        summon[Monad[F]].pure(
          Right[ManagementServiceError, List[Statement]](
            emitStatementsFromSchema(
              previousEntityTypeIRI,
              previousEntityType.schema
            )
          )
        )
      )
      previousStatements = allPreviousStatements.filter(statement =>
        statement.getPredicate.toString.endsWith("constraints")
      )
      allStatements <- EitherT(
        summon[Monad[F]].pure(
          Right[ManagementServiceError, List[Statement]](
            emitStatementsFromSchema(instanceType, entityTypeRequest.schema)
          )
        )
      )
      statements = allStatements.filter(statement =>
        statement.getPredicate.toString.endsWith("constraints")
      )
      _ <- EitherT(
        summon[Functor[F]].map(
          repository.removeAndInsertStatements(statements, previousStatements)
        )(_ => Right[ManagementServiceError, Unit](()))
      )
      _ <- EitherT(
        summon[Functor[F]].map(
          cache.insert(entityTypeRequest.name, entityTypeRequest)
        )(_ => Right[ManagementServiceError, Unit](()))
      )
    } yield ()).value
  end updateConstraints

  override def exist(
      instanceTypeName: String
  ): F[Either[ManagementServiceError, Boolean]] =
    val res = repository.evaluateQuery(s"""
         |PREFIX ns:  <${ns.getName}>
         |PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
         |PREFIX owl: <http://www.w3.org/2002/07/owl#>
         |SELECT ?a WHERE {
         |   ?a rdf:type owl:NamedIndividual .
         |   ?a rdf:type ns:EntityType .
         |   ?a ns:typeName "$instanceTypeName"^^<http://www.w3.org/2001/XMLSchema#string> .
         |}
         |""".stripMargin)
    summon[Functor[F]].map(res)(res => {
      val count = res.toList.length
      if count > 0
      then Right[ManagementServiceError, Boolean](true)
      else Right[ManagementServiceError, Boolean](false)
    })
  end exist

end TypeManagementServiceInterpreter
