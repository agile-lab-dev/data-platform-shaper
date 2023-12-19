package it.agilelab.dataplatformshaper.domain.model.schema

import org.apache.calcite.avatica.util.Casing
import org.apache.calcite.sql.*
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.parser.impl.SqlParserImpl

import java.util.UUID
import scala.annotation.targetName
import scala.jdk.CollectionConverters.*

trait SearchPredicateTerm

given StringToSearchPredicateAttribute
    : Conversion[String, SearchPredicateAttribute] with
  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.throw"
    )
  )
  override def apply(string: String): SearchPredicateAttribute =
    if string.contains('/') then
      throw new IllegalArgumentException(s"$string should not contain any '/'")
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
  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.throw"
    )
  )
  def /(subPath: String): SearchPredicateAttribute =
    if subPath.contains('/') then
      throw new IllegalArgumentException(s"$subPath should not contain any '/'")
    SearchPredicateAttribute(subPath.trim :: namedAttribute.attributePath)
  end /
end extension

@inline private def genId = UUID.randomUUID().toString.replace("-", "")

case class SearchPredicateAttribute(attributePath: List[String])
    extends SearchPredicateTerm:

  def /(subPath: SearchPredicateAttribute): SearchPredicateAttribute =
    SearchPredicateAttribute(subPath.attributePath ::: this.attributePath)
  end /

  def =:=(value: SearchPredicateValue[AnyVal]): SearchPredicate =
    SearchPredicate(
      {
        val s = genId
        val p = genId
        val v = genId
        s"""
         |{
         |  ?$s ?$p ?$v
         |  FILTER (?$p = iri("https://w3id.org/agile-dm/ontology/${attributePath.reverse
            .mkString("/")}") && ?$v = ${value.value.toString} ) .
         |}
         |""".stripMargin
      }
    )

  @targetName("stringEqual")
  def =:=(value: SearchPredicateValue[String]): SearchPredicate =
    SearchPredicate(
      {
        val s = genId
        val p = genId
        val v = genId
        s"""
         |{
         |  ?$s ?$p ?$v
         |  FILTER (?$p = iri("https://w3id.org/agile-dm/ontology/${attributePath.reverse
            .mkString("/")}") && ?$v = "${value.value}"^^xsd:string ) .
         |}
         |""".stripMargin
      }
    )

  private def generateComparison(
      comparisonOperator: String,
      value: AnyVal
  ): SearchPredicate = SearchPredicate(
    {
      val s = genId
      val p = genId
      val v = genId
      s"""
         |{
         |  ?$s ?$p ?$v
         |  FILTER (?$p = iri("https://w3id.org/agile-dm/ontology/${attributePath.reverse
          .mkString("/")}") && ?$v $comparisonOperator ${value.toString} ) .
         |}
         |""".stripMargin
    }
  )

  def <(value: SearchPredicateValue[AnyVal]): SearchPredicate =
    generateComparison("<", value.value)

  def <=(value: SearchPredicateValue[AnyVal]): SearchPredicate =
    generateComparison("<=", value.value)

  def >(value: SearchPredicateValue[AnyVal]): SearchPredicate =
    generateComparison(">", value.value)

  def >=(value: SearchPredicateValue[AnyVal]): SearchPredicate =
    generateComparison(">=", value.value)

end SearchPredicateAttribute

case class SearchPredicate(querySegment: String) extends SearchPredicateTerm:
  def &&(pred: SearchPredicate): SearchPredicate =
    SearchPredicate(
      s"""
         |{
         |   ${this.querySegment}
         |   ${pred.querySegment}
         |}
         |""".stripMargin
    )
  end &&

  def ||(pred: SearchPredicate): SearchPredicate =
    SearchPredicate(
      s"""
         |{
         |   {
         |     ${this.querySegment}
         |   } union {
         |     ${pred.querySegment}
         |   }
         |}
         |""".stripMargin
    )
  end ||
end SearchPredicate

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.asInstanceOf"
  )
)
def generateSearchPredicate(query: String): SearchPredicate =

  val sqlParserConfig = SqlParser
    .config()
    .withCaseSensitive(true)
    .withParserFactory(SqlParserImpl.FACTORY)
    .withQuotedCasing(Casing.UNCHANGED)
    .withUnquotedCasing(Casing.UNCHANGED)

  val node = SqlParser.create(query, sqlParserConfig).parseExpression()

  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.throw"
    )
  )
  def generateCode(node: SqlNode): SearchPredicateTerm =
    node match
      case call: SqlCall =>
        val operator = call.getOperator
        operator.kind match
          case SqlKind.AND =>
            val operandList = call.getOperandList.asScala.toList
            val head = operandList.head
            val tail = operandList.tail
            tail.foldRight(generateCode(head))((n1, n2) =>
              generateCode(n1)
                .asInstanceOf[SearchPredicate]
                .&&(n2.asInstanceOf[SearchPredicate])
            )
          case SqlKind.OR =>
            val operandList = call.getOperandList.asScala.toList
            val head = operandList.head
            val tail = operandList.tail
            tail.foldRight(generateCode(head))((n1, n2) =>
              generateCode(n1)
                .asInstanceOf[SearchPredicate]
                .||(n2.asInstanceOf[SearchPredicate])
            )
          case SqlKind.EQUALS =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            operand2 match
              case _: SqlCharStringLiteral =>
                generateCode(operand1)
                  .asInstanceOf[SearchPredicateAttribute] =:= generateCode(
                  operand2
                ).asInstanceOf[SearchPredicateValue[String]]
              case _: SqlNumericLiteral =>
                generateCode(operand1)
                  .asInstanceOf[SearchPredicateAttribute] =:= generateCode(
                  operand2
                ).asInstanceOf[SearchPredicateValue[AnyVal]]
            end match
          case SqlKind.LESS_THAN =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            generateCode(operand1)
              .asInstanceOf[SearchPredicateAttribute] < generateCode(operand2)
              .asInstanceOf[SearchPredicateValue[AnyVal]]
          case SqlKind.LESS_THAN_OR_EQUAL =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            generateCode(operand1)
              .asInstanceOf[SearchPredicateAttribute] <= generateCode(operand2)
              .asInstanceOf[SearchPredicateValue[AnyVal]]
          case SqlKind.GREATER_THAN =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            generateCode(operand1)
              .asInstanceOf[SearchPredicateAttribute] > generateCode(operand2)
              .asInstanceOf[SearchPredicateValue[AnyVal]]
          case SqlKind.GREATER_THAN_OR_EQUAL =>
            val operandList = call.getOperandList.asScala.toList
            val operand1 = operandList.head
            val operand2 = operandList.tail.head
            generateCode(operand1)
              .asInstanceOf[SearchPredicateAttribute] >= generateCode(operand2)
              .asInstanceOf[SearchPredicateValue[AnyVal]]
          case SqlKind.DIVIDE =>
            val operandList = call.getOperandList.asScala.toList.reverse
            val head = operandList.head
            val tail = operandList.tail
            tail.foldRight(generateCode(head))((n1, n2) =>
              generateCode(n1)
                .asInstanceOf[SearchPredicateAttribute]
                ./(n2.asInstanceOf[SearchPredicateAttribute])
            )
          case _ =>
            throw new IllegalStateException()
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
      case _ =>
        throw new IllegalStateException()
    end match
  end generateCode

  generateCode(node).asInstanceOf[SearchPredicate]
end generateSearchPredicate
