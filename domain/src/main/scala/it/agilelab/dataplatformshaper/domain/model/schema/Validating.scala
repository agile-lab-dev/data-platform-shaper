package it.agilelab.dataplatformshaper.domain.model.schema

def generateCueModel(schema: Schema): String =
  def internalGenerateCueModel(
      schema: StructType,
      currentIndentation: Int
  ): String =
    val stringBuilder = new StringBuilder()

    @inline def sanitizeAttributeName(attributeName: String) =
      if attributeName.contains("-") then s"'$attributeName'" else attributeName
    end sanitizeAttributeName

    @inline def blanks: Int ?=> String = n ?=> List.fill(n)(' ').mkString

    def genCueConstraintsForAttribute(
        attributeName: String,
        mode: Mode,
        constraints: Option[String],
        cueType: String
    )(using currentIndentation: Int) = {
      mode match
        case Mode.Required =>
          stringBuilder.append(
            s"$blanks${sanitizeAttributeName(attributeName)}!: $cueType ${constraints
                .fold("")(constr => s"& ($constr)")}"
          )
        case Mode.Repeated =>
          stringBuilder.append(
            s"$blanks${sanitizeAttributeName(attributeName)}: [ ...$cueType ${constraints
                .fold("")(constr => s"& ($constr)")}]"
          )
        case Mode.Nullable =>
          stringBuilder.append(
            s"$blanks${sanitizeAttributeName(attributeName)}?: null | ($cueType ${constraints
                .fold("")(constr => s"& ($constr)")})"
          )
      end match
    }

    schema.records.foreach(rec =>
      given n: Int = currentIndentation
      rec match
        case (attributeName, StringType(mode, constraints)) =>
          genCueConstraintsForAttribute(
            attributeName,
            mode,
            constraints,
            "string"
          )
        case (attributeName, IntType(mode, constraints)) =>
          genCueConstraintsForAttribute(attributeName, mode, constraints, "int")
        case (attributeName, LongType(mode, constraints)) =>
          genCueConstraintsForAttribute(attributeName, mode, constraints, "int")
        case (attributeName, FloatType(mode, constraints)) =>
          genCueConstraintsForAttribute(
            attributeName,
            mode,
            constraints,
            "float"
          )
        case (attributeName, DoubleType(mode, constraints)) =>
          genCueConstraintsForAttribute(
            attributeName,
            mode,
            constraints,
            "float"
          )
        case (attributeName, BooleanType(mode, constraints)) =>
          genCueConstraintsForAttribute(
            attributeName,
            mode,
            constraints,
            "bool"
          )
        case (attributeName, SqlDecimal(mode, constraints)) =>
          genCueConstraintsForAttribute(
            attributeName,
            mode,
            constraints,
            "number"
          )
        case (attributeName, DateType(mode, constraints)) =>
          genCueConstraintsForAttribute(
            attributeName,
            mode,
            constraints,
            "string"
          )
        case (attributeName, TimestampDataType(mode, constraints)) =>
          genCueConstraintsForAttribute(
            attributeName,
            mode,
            constraints,
            "string"
          )
        case (attributeName, JsonType(mode)) =>
          genCueConstraintsForAttribute(attributeName, mode, None, "string")
        case (attributeName, str @ StructType(_, mode)) =>
          given n: Int = currentIndentation
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitizeAttributeName(attributeName)}!: {\n${internalGenerateCueModel(str, n + 2)}}"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitizeAttributeName(attributeName)}: [ ...{\n${internalGenerateCueModel(str, n + 2)}$blanks}]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitizeAttributeName(attributeName)}?: {\n${internalGenerateCueModel(str, n + 2)}}"
              )
          end match
      end match
      stringBuilder.append('\n')
    )
    stringBuilder.result()
  end internalGenerateCueModel

  internalGenerateCueModel(schema: StructType, 0)
end generateCueModel
