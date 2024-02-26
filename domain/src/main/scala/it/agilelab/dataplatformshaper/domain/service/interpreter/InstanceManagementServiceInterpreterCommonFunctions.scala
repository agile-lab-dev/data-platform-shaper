package it.agilelab.dataplatformshaper.domain.service.interpreter
import cats.*
import cats.effect.*
import cats.implicits.*
import io.circe.Json
import it.agilelab.dataplatformshaper.domain.knowledgegraph.KnowledgeGraph
import it.agilelab.dataplatformshaper.domain.model.NS.{L3, ns}
import it.agilelab.dataplatformshaper.domain.model.schema.DataType.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.model.schema.parsing.FoldingPhase
import it.agilelab.dataplatformshaper.domain.model.schema.parsing.FoldingPhase.*
import it.agilelab.dataplatformshaper.domain.model.schema.{
  DataType,
  Schema,
  unfoldTuple
}
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.TupleIsNotConformToSchema
import org.eclipse.rdf4j.model.util.Statements.statement
import org.eclipse.rdf4j.model.util.Values.{iri, literal, triple}
import org.eclipse.rdf4j.model.{IRI, Statement}
import org.typelevel.log4cats.Logger

import java.time.{LocalDate, ZonedDateTime}
import java.util.UUID

trait InstanceManagementServiceInterpreterCommonFunctions[F[_]: Sync]:

  self: InstanceManagementServiceInterpreter[F] =>

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.var"
    )
  )
  def emmitStatementsForEntity(
      entityId: String,
      initialStatements: List[Statement],
      tuple: Tuple,
      schema: Schema
  ): Either[ManagementServiceError, List[Statement]] =

    val entity = iri(ns, entityId)
    val previousEntityIriStack = collection.mutable.Stack(entity)
    var currentEntityIri = entity
    var statements = List.empty[Statement] ++ initialStatements

    @SuppressWarnings(
      Array(
        "scalafix:DisableSyntax.asInstanceOf"
      )
    )
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
            L3
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
            L3
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
            L3
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
            L3
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
            L3
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
            L3
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
            L3
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
            L3
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
            L3
          ) :: statements
        case StructType(_, _) =>
          foldingPhase match
            case BeginFoldingStruct =>
              val structIri = iri(ns, UUID.randomUUID.toString)
              statements = statement(
                triple(currentEntityIri, iri(ns, currentPath), structIri),
                L3
              ) :: statements
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

    unfoldTuple(
      tuple,
      schema,
      internalEmitStatements
    ) match
      case Left(parsingError) =>
        Left[ManagementServiceError, List[Statement]](
          TupleIsNotConformToSchema(parsingError)
        )
      case Right(_) =>
        Right[ManagementServiceError, List[Statement]](statements)
    end match

  end emmitStatementsForEntity

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

  def fetchFieldsForInstance(
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
         |    FILTER ( ?value not in ( ns:Entity ))
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

  def handlePrimitiveDataTypes(
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
    Applicative[F].pure(tuple)
  end handlePrimitiveDataTypes

  def handleStructDataType(
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
              Applicative[F].pure(fieldName -> Option.empty[Tuple])
            else
              val tuple =
                createTupleForStructDataType(logger, dataType, fieldValue.head)
              tuple.map(t => fieldName -> Some(t))
      case None =>
        Applicative[F].pure(fieldName -> None)
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
          case _ => Applicative[F].pure(EmptyTuple)
      )
    Traverse[List].sequence(tuples).map(_.fold(EmptyTuple)(_ :* _))
  end fieldsToTuple
end InstanceManagementServiceInterpreterCommonFunctions
