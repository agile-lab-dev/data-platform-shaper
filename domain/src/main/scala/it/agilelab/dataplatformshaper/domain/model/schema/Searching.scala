package it.agilelab.dataplatformshaper.domain.model.schema

package searching:

  import java.util.UUID

  extension (attributePath: String)

    def ===(value: Any): NamedAttributePredicate =
      value match
        case v: String =>
          NamedAttribute(List(attributePath)) === v
        case v: AnyVal =>
          NamedAttribute(List(attributePath)) === v
      end match
    end ===

    def <(value: AnyVal): NamedAttributePredicate = NamedAttribute(List(attributePath)) < value

    def /(subPath: String): NamedAttribute =
      if subPath.contains("/") then
        throw new IllegalArgumentException(s"$subPath should not contain any '/'")
      if subPath.contains("/") then
        throw new IllegalArgumentException(s"$attributePath should not contain any '/'")
      NamedAttribute(List(subPath, attributePath))
    end /

  end extension

  extension (namedAttribute: NamedAttribute)
    def /(subPath: String): NamedAttribute =
      NamedAttribute(subPath :: namedAttribute.attributePath)
    end /
  end extension

  case class NamedAttribute(attributePath: List[String]):

    def ===(value: AnyVal): NamedAttributePredicate = NamedAttributePredicate(
      {
        val s = UUID.randomUUID().toString.replace("-","")
        val p = UUID.randomUUID().toString.replace("-","")
        val v = UUID.randomUUID().toString.replace("-","")
        s"""
           |{
           |  ?$s ?$p ?$v
           |  FILTER (?$p = iri("https://w3id.org/agile-dm/ontology/${attributePath.reverse.mkString("/")}") && ?$v = ${value.toString} ) .
           |}
           |""".stripMargin
      }
    )

    def ===(value: String): NamedAttributePredicate = NamedAttributePredicate(
      {
        val s = UUID.randomUUID().toString.replace("-", "")
        val p = UUID.randomUUID().toString.replace("-", "")
        val v = UUID.randomUUID().toString.replace("-", "")
        s"""
           |{
           |  ?$s ?$p ?$v
           |  FILTER (?$p = iri("https://w3id.org/agile-dm/ontology/${attributePath.reverse.mkString("/")}") && ?$v = "$value"^^xsd:string ) .
           |}
           |""".stripMargin
      }
    )

    private def generateComparison(comparisonOperator: String, value: AnyVal): NamedAttributePredicate = NamedAttributePredicate(
      {
        val s = UUID.randomUUID().toString.replace("-", "")
        val p = UUID.randomUUID().toString.replace("-", "")
        val v = UUID.randomUUID().toString.replace("-", "")
        s"""
           |{
           |  ?$s ?$p ?$v
           |  FILTER (?$p = iri("https://w3id.org/agile-dm/ontology/${attributePath.reverse.mkString("/")}") && ?$v $comparisonOperator ${value.toString} ) .
           |}
           |""".stripMargin
      }
    )

    def <(value: AnyVal): NamedAttributePredicate = generateComparison("<", value)

    def <=(value: AnyVal): NamedAttributePredicate = generateComparison("<=", value)

    def >(value: AnyVal): NamedAttributePredicate = generateComparison(">", value)

    def >=(value: AnyVal): NamedAttributePredicate = generateComparison(">=", value)

  end NamedAttribute

  case class NamedAttributePredicate(querySegment: String):
    def &&(pred: NamedAttributePredicate): NamedAttributePredicate =
      NamedAttributePredicate(
        s"""
           |{
           |   ${this.querySegment}
           |   ${pred.querySegment}
           |}
           |""".stripMargin
      )
    end &&

    def ||(pred: NamedAttributePredicate): NamedAttributePredicate =
      NamedAttributePredicate(
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
  end NamedAttributePredicate

end searching