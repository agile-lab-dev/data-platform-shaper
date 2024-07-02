package it.agilelab.dataplatformshaper.domain.model.schema

import it.agilelab.dataplatformshaper.domain.model.NS.ns
import it.agilelab.dataplatformshaper.domain.model.Relationship
import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.sql.*
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.impl.SqlParserImpl

import java.util.UUID
import scala.annotation.{tailrec, targetName}
import scala.jdk.CollectionConverters.*

trait SearchPredicateTerm

given StringToSearchPredicateAttribute
  : Conversion[String, SearchPredicateAttribute] with
  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  override def apply(string: String): SearchPredicateAttribute =
    if string.contains('/') then
      throw IllegalArgumentException(s"$string should not contain any '/'")
    SearchPredicateAttribute(List(string))
  end apply

case class SearchPredicateValue[T](value: T) extends SearchPredicateTerm

given StringToSearchPredicateValue
  : Conversion[String, SearchPredicateValue[String]] with
  override def apply(string: String): SearchPredicateValue[String] =
    SearchPredicateValue(string)

given AnyValToSearchPredicateValue
  : Conversion[AnyVal, SearchPredicateValue[AnyVal]] with
  override def apply(anyVal: AnyVal): SearchPredicateValue[AnyVal] =
    SearchPredicateValue(anyVal)

extension (namedAttribute: SearchPredicateAttribute)
  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  def /(subPath: String): SearchPredicateAttribute =
    if subPath.contains('/') then
      throw IllegalArgumentException(s"$subPath should not contain any '/'")
    SearchPredicateAttribute(subPath.trim :: namedAttribute.attributePath)
  end /
end extension

@inline private def genId = UUID.randomUUID().toString.replace("-", "")

case class SearchPredicateAttribute(attributePath: List[String])
    extends SearchPredicateTerm:

  def /(subPath: SearchPredicateAttribute): SearchPredicateAttribute =
    SearchPredicateAttribute(subPath.attributePath ::: this.attributePath)
  end /

  def generateAttributeClauses(
    i: String,
    filter: Option[String => String]
  ): String =
    val attributeRels = attributePath.reverse.inits.toList
      .filter(_.nonEmpty)
      .map(l => s"ns:${l.mkString("\\/")}")
    val stringBuffer = StringBuffer()

    @tailrec
    def loopOnAttributeRels(
      i: String,
      o: String,
      filter: Option[String => String],
      attributeRels: List[String]
    ): Unit =
      attributeRels match
        case head1 :: head2 :: tail =>
          val s = genId
          stringBuffer.append(s"?$s $head1 ?$o .")
          stringBuffer.append('\n')
          stringBuffer.append(s"${filter.fold("")(f => f(o))}")
          stringBuffer.append('\n')
          loopOnAttributeRels(i, s, None, head2 :: tail)
        case head :: Nil =>
          stringBuffer.append(s"?$i $head ?$o .")
          stringBuffer.append('\n')
          stringBuffer.append(s"${filter.fold("")(f => f(o))}")
          ()
        case Nil =>
          ()
    end loopOnAttributeRels

    loopOnAttributeRels(i, genId, filter, attributeRels)
    stringBuffer.toString
  end generateAttributeClauses

  def =:=(value: SearchPredicateValue[AnyVal]): String ?=> SearchPredicate =
    (i: String) ?=>
      SearchPredicate({
        s"""
         |{
         |  ${generateAttributeClauses(
            i,
            Some(v => s"FILTER (?$v = ${value.value.toString} ) .")
          )}
         |}
         |""".stripMargin
      })
  end =:=

  def =!=(value: SearchPredicateValue[AnyVal]): String ?=> SearchPredicate =
    (i: String) ?=>
      SearchPredicate({
        s"""
           |{
           |  ${generateAttributeClauses(
            i,
            Some(v => s"FILTER (?$v != ${value.value.toString} ) .")
          )}
           |}
           |""".stripMargin
      })
  end =!=

  @targetName("stringEqual")
  def =:=(value: SearchPredicateValue[String]): String ?=> SearchPredicate =
    (i: String) ?=>
      SearchPredicate({
        s"""
           |{
           |  ${generateAttributeClauses(
            i,
            Some(v => s"""FILTER (?$v = "${value.value}"^^xsd:string ) .""")
          )}
           |}
           |""".stripMargin
      })
  end =:=

  @targetName("stringDifferent")
  def =!=(value: SearchPredicateValue[String]): String ?=> SearchPredicate =
    (i: String) ?=>
      SearchPredicate({
        s"""
           |{
           |  ${generateAttributeClauses(
            i,
            Some(v => s"""FILTER (?$v != "${value.value}"^^xsd:string ) .""")
          )}
           |}
           |""".stripMargin
      })
  end =!=

  def like(value: SearchPredicateValue[String]): String ?=> SearchPredicate =
    (i: String) ?=>
      SearchPredicate({
        s"""
           |{
           |  ${generateAttributeClauses(
            i,
            Some(v => s"""FILTER REGEX(?$v, "${value.value}") .""")
          )}
           |}
           |""".stripMargin
      })
  end like

  private def generateComparison(
    comparisonOperator: String,
    value: AnyVal
  ): String ?=> SearchPredicate = (i: String) ?=>
    SearchPredicate({
      s"""
         |{
         |  ${generateAttributeClauses(
          i,
          Some(v => s"FILTER (?$v $comparisonOperator ${value.toString} ) .")
        )}
         |}
         |""".stripMargin
    })

  def <(value: SearchPredicateValue[AnyVal]): String ?=> SearchPredicate =
    (i: String) ?=> generateComparison("<", value.value)

  def <=(value: SearchPredicateValue[AnyVal]): String ?=> SearchPredicate =
    (i: String) ?=> generateComparison("<=", value.value)

  def >(value: SearchPredicateValue[AnyVal]): String ?=> SearchPredicate =
    (i: String) ?=> generateComparison(">", value.value)

  def >=(value: SearchPredicateValue[AnyVal]): String ?=> SearchPredicate =
    (i: String) ?=> generateComparison(">=", value.value)

end SearchPredicateAttribute

case class SearchPredicate(querySegment: String) extends SearchPredicateTerm:
  def &&(pred: SearchPredicate): SearchPredicate =
    SearchPredicate(s"""
         |{
         |   ${this.querySegment}
         |   ${pred.querySegment}
         |}
         |""".stripMargin)
  end &&

  def ||(pred: SearchPredicate): SearchPredicate =
    SearchPredicate(s"""
         |{
         |   {
         |     ${this.querySegment}
         |   } union {
         |     ${pred.querySegment}
         |   }
         |}
         |""".stripMargin)
  end ||
end SearchPredicate

@SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
def generateSearchPredicate(
  instancePlaceHolder: String,
  query: String
): SearchPredicate =

  val sqlParserConfig = SqlParser
    .config()
    .withCaseSensitive(true)
    .withParserFactory(SqlParserImpl.FACTORY)
    .withQuotedCasing(Casing.UNCHANGED)
    .withUnquotedCasing(Casing.UNCHANGED)

  val node = SqlParser.create(query, sqlParserConfig).parseExpression()

  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  def generateCode(
    instancePlaceHolder: String,
    node: SqlNode
  ): SearchPredicateTerm =

    given i: String = instancePlaceHolder

    node match
      case call: SqlCall =>
        val operator = call.getOperator
        operator.kind match
          case SqlKind.AND =>
            val operandList = call.getOperandList.asScala.toList
            val head = operandList.head
            val tail = operandList.tail
            tail.foldRight(generateCode(instancePlaceHolder, head))((n1, n2) =>
              generateCode(instancePlaceHolder, n1)
                .asInstanceOf[SearchPredicate]
                .&&(n2.asInstanceOf[SearchPredicate])
            )
          case SqlKind.OR =>
            val operandList = call.getOperandList.asScala.toList
            val head = operandList.head
            val tail = operandList.tail
            tail.foldRight(generateCode(instancePlaceHolder, head))((n1, n2) =>
              generateCode(instancePlaceHolder, n1)
                .asInstanceOf[SearchPredicate]
                .||(n2.asInstanceOf[SearchPredicate])
            )
          case SqlKind.EQUALS =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            operand2 match
              case _: SqlCharStringLiteral =>
                generateCode(instancePlaceHolder, operand1)
                  .asInstanceOf[SearchPredicateAttribute] =:= generateCode(
                  instancePlaceHolder,
                  operand2
                ).asInstanceOf[SearchPredicateValue[String]]
              case _: SqlNumericLiteral =>
                generateCode(instancePlaceHolder, operand1)
                  .asInstanceOf[SearchPredicateAttribute] =:= generateCode(
                  instancePlaceHolder,
                  operand2
                ).asInstanceOf[SearchPredicateValue[AnyVal]]
              case b: SqlLiteral if List("true", "false").contains(b.toValue) =>
                generateCode(instancePlaceHolder, operand1)
                  .asInstanceOf[SearchPredicateAttribute] =:= generateCode(
                  instancePlaceHolder,
                  operand2
                ).asInstanceOf[SearchPredicateValue[AnyVal]]
            end match
          case SqlKind.NOT_EQUALS =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            operand2 match
              case _: SqlCharStringLiteral =>
                generateCode(instancePlaceHolder, operand1)
                  .asInstanceOf[SearchPredicateAttribute] =!= generateCode(
                  instancePlaceHolder,
                  operand2
                ).asInstanceOf[SearchPredicateValue[String]]
              case _: SqlNumericLiteral =>
                generateCode(instancePlaceHolder, operand1)
                  .asInstanceOf[SearchPredicateAttribute] =!= generateCode(
                  instancePlaceHolder,
                  operand2
                ).asInstanceOf[SearchPredicateValue[AnyVal]]
            end match
          case SqlKind.LESS_THAN =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            generateCode(instancePlaceHolder, operand1)
              .asInstanceOf[SearchPredicateAttribute] < generateCode(
              instancePlaceHolder,
              operand2
            )
              .asInstanceOf[SearchPredicateValue[AnyVal]]
          case SqlKind.LESS_THAN_OR_EQUAL =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            generateCode(instancePlaceHolder, operand1)
              .asInstanceOf[SearchPredicateAttribute] <= generateCode(
              instancePlaceHolder,
              operand2
            )
              .asInstanceOf[SearchPredicateValue[AnyVal]]
          case SqlKind.GREATER_THAN =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            generateCode(instancePlaceHolder, operand1)
              .asInstanceOf[SearchPredicateAttribute] > generateCode(
              instancePlaceHolder,
              operand2
            )
              .asInstanceOf[SearchPredicateValue[AnyVal]]
          case SqlKind.GREATER_THAN_OR_EQUAL =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            generateCode(instancePlaceHolder, operand1)
              .asInstanceOf[SearchPredicateAttribute] >= generateCode(
              instancePlaceHolder,
              operand2
            )
              .asInstanceOf[SearchPredicateValue[AnyVal]]
          case SqlKind.LIKE =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            generateCode(instancePlaceHolder, operand1)
              .asInstanceOf[SearchPredicateAttribute] `like` generateCode(
              instancePlaceHolder,
              operand2
            )
              .asInstanceOf[SearchPredicateValue[String]]
          case SqlKind.DIVIDE =>
            val operandList = call.getOperandList.asScala.toList.reverse
            val head = operandList.head
            val tail = operandList.tail
            tail.foldRight(generateCode(instancePlaceHolder, head))((n1, n2) =>
              generateCode(instancePlaceHolder, n1)
                .asInstanceOf[SearchPredicateAttribute]
                ./(n2.asInstanceOf[SearchPredicateAttribute])
            )
          case _ =>
            throw IllegalStateException()
        end match
      case id: SqlIdentifier =>
        SearchPredicateAttribute(List(s"""${id.toString}"""))
      case string: SqlCharStringLiteral =>
        SearchPredicateValue[String](
          s"""${string.toString.replaceAll("'", "")}"""
        )
      case num: SqlNumericLiteral =>
        val value =
          try num.toString.toLong
          catch case _ => num.toString.toDouble
        SearchPredicateValue[AnyVal](value)
      case b: SqlLiteral if List("true", "false").contains(b.toValue) =>
        SearchPredicateValue[String](s"""${b.toString.replaceAll("'", "")}""")
      case _ =>
        throw IllegalStateException()
    end match
  end generateCode

  generateCode(instancePlaceHolder, node).asInstanceOf[SearchPredicate]
end generateSearchPredicate

private enum PathElementType:
  case Source, Start, Relationship, EntityType

private def getSinglePathElementType(pathElement: String): PathElementType =
  pathElement match
    case p: String if p.contains("source") => PathElementType.Source
    case p: String
        if p.contains("#") && Relationship.isRelationship(p.split("#")(0)) =>
      PathElementType.Relationship
    case p: String if Relationship.isRelationship(p) =>
      PathElementType.Relationship
    case _ => PathElementType.EntityType
end getSinglePathElementType

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.defaultArgs",
    "scalafix:DisableSyntax.var",
    "scalafix:DisableSyntax.throw"
  )
)
def getPathQuery(
  splitPath: List[String],
  initialInstanceId: String,
  previousID: String = "",
  previousPathElementType: PathElementType = PathElementType.Start
): (String, String) =
  var finalInstanceID: String = ""
  def loop(
    innerSplitPath: List[String],
    innerPreviousPathElementType: PathElementType,
    innerInitialInstanceId: String,
    previousID: String = ""
  ): String =
    val firstPathElement = innerSplitPath.head
    val firstPathElementType = getSinglePathElementType(firstPathElement)
    innerSplitPath match
      case Nil => ""
      case head :: Nil =>
        val entityTypeName = head
        val finalPathElementType = getSinglePathElementType(entityTypeName)
        if (finalPathElementType.equals(
            PathElementType.EntityType
          ) && innerPreviousPathElementType.equals(
            PathElementType.Relationship
          )) || (finalPathElementType.equals(
            PathElementType.Source
          ) && finalPathElementType.equals(PathElementType.Start))
        then
          val splitPathElement = head.split("\\.find\\(\"").toList
          finalInstanceID = previousID
          splitPathElement.length match
            case 2 =>
              val entityTypeName = splitPathElement.head
              val searchString = splitPathElement(1).stripSuffix("\")")
              val searchPredicate =
                generateSearchPredicate(s"$previousID", searchString)
              val entityTypeQuery =
                s"""
                   | ?$previousID ns:isClassifiedBy ns:$entityTypeName .
                   |""".stripMargin
              searchPredicate.querySegment + entityTypeQuery
            case 1 =>
              val entityTypeName = splitPathElement.head
              val entityTypeQuery =
                s"""
                   | ?$previousID ns:isClassifiedBy ns:$entityTypeName .
                   |""".stripMargin
              entityTypeQuery
            case _ =>
              throw IllegalArgumentException("Expected only one find clause")
        else
          throw IllegalArgumentException(
            "Expected Relationship/EntityType or source at the end of the path"
          )
      case head :: tail
          if firstPathElementType.equals(PathElementType.Source) =>
        if innerPreviousPathElementType.equals(PathElementType.Start) then
          val instanceID = UUID.randomUUID().toString.replace("-", "")
          val entityID = UUID.randomUUID().toString.replace("-", "")
          val sourceQuery =
            s"""
               | ?$instanceID ns:isClassifiedBy ?$entityID .
               | ?$entityID rdf:type ns:EntityType .
               | FILTER(?$instanceID = <${ns.getName}$innerInitialInstanceId>)
               |""".stripMargin
          val nextPathQuery =
            loop(tail, firstPathElementType, innerInitialInstanceId, instanceID)
          sourceQuery + nextPathQuery
        else
          throw IllegalArgumentException(
            "Expected source to be at the start of the path"
          )
      case head :: tail
          if firstPathElementType.equals(PathElementType.Relationship) =>
        if innerPreviousPathElementType.equals(
            PathElementType.EntityType
          ) || innerPreviousPathElementType.equals(PathElementType.Source)
        then
          head match
            case head if head.startsWith("mappedTo") =>
              val relationshipID = UUID.randomUUID().toString.replace("-", "")
              val firstEntityID = UUID.randomUUID().toString.replace("-", "")
              val relationshipQuery =
                s"""
                   | ?$previousID ?$relationshipID ?$firstEntityID .
                   | ?$firstEntityID rdf:type ns:Entity .
                   | FILTER(STRSTARTS(STR(?$relationshipID), "${ns.getName}mappedTo#"))
                   |""".stripMargin
              val nextPathQuery =
                loop(
                  tail,
                  firstPathElementType,
                  innerInitialInstanceId,
                  firstEntityID
                )
              relationshipQuery + nextPathQuery
            case head if head.equals("hasPart") || head.equals("partOf") =>
              val instanceID = UUID.randomUUID().toString.replace("-", "")
              val relationshipID = UUID.randomUUID().toString.replace("-", "")
              val relationshipQuery =
                s"""
                   | ?$previousID ?$relationshipID ?$instanceID .
                   | FILTER(?$relationshipID = <${Relationship.hasPart.getNamespace}$head>) .
                   |""".stripMargin
              val nextPathQuery =
                loop(
                  tail,
                  firstPathElementType,
                  innerInitialInstanceId,
                  instanceID
                )
              relationshipQuery + nextPathQuery
            case _ =>
              val instanceID = UUID.randomUUID().toString.replace("-", "")
              val relationshipQuery =
                s"""
                   | ?$previousID ns:$head ?$instanceID .
                   |""".stripMargin
              val nextPathQuery =
                loop(
                  tail,
                  firstPathElementType,
                  innerInitialInstanceId,
                  instanceID
                )
              relationshipQuery + nextPathQuery
        else
          throw IllegalArgumentException(
            "Expected Source or EntityType before Relationship in path"
          )
      case head :: tail
          if firstPathElementType.equals(PathElementType.EntityType) =>
        if innerPreviousPathElementType.equals(PathElementType.Relationship)
        then
          val splitPathElement = head.split("\\.find\\(\"").toList
          splitPathElement.length match
            case 2 =>
              val entityTypeName = splitPathElement.head
              val searchString = splitPathElement(1).stripSuffix("\")")
              val searchPredicate =
                generateSearchPredicate(s"$previousID", searchString)
              val entityTypeQuery =
                s"""
                   | ?$previousID ns:isClassifiedBy ns:$entityTypeName .
                   |""".stripMargin
              val nextPathQuery = loop(
                tail,
                firstPathElementType,
                innerInitialInstanceId,
                previousID
              )
              searchPredicate.querySegment + entityTypeQuery + nextPathQuery
            case 1 =>
              val entityTypeName = splitPathElement.head
              val entityTypeQuery =
                s"""
                   | ?$previousID ns:isClassifiedBy ns:$entityTypeName .
                   |""".stripMargin
              val nextPathQuery = loop(
                tail,
                firstPathElementType,
                innerInitialInstanceId,
                previousID
              )
              entityTypeQuery + nextPathQuery
            case _ =>
              throw IllegalArgumentException("Expected only one find clause")
        else
          throw IllegalArgumentException(
            "Expected Relationship before an EntityType"
          )
      case _ => throw IllegalArgumentException("Unexpected element in path")
  end loop
  val result =
    loop(splitPath, previousPathElementType, initialInstanceId, previousID)
  (result, finalInstanceID)
end getPathQuery
