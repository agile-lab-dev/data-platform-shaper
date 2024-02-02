package it.agilelab.dataplatformshaper.domain.model.schema

def generateCueModel(schema: Schema): String =
  def internalGenerateCueModel(
      schema: StructType,
      currentIndentation: Int
  ): String =
    val stringBuilder = new StringBuilder()

    @inline def sanitezeAttributName(attributeName: String) =
      if attributeName.contains("-") then s"'$attributeName'" else attributeName
    end sanitezeAttributName

    @inline def blanks: Int ?=> String = n ?=> List.fill(n)(' ').mkString

    schema.records.foreach(rec =>
      given n: Int = currentIndentation
      rec match
        case (attributeName, StringType(mode, constraints)) =>
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}!: string ${constraints
                    .fold("")(constr => s"& ($constr)")}"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}: [ ...string ${constraints
                    .fold("")(constr => s"& ($constr)")}]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}?: null | (string ${constraints
                    .fold("")(constr => s"& ($constr)")})"
              )
          end match
        case (attributeName, IntType(mode, constraints)) =>
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}!: int ${constraints
                    .fold("")(constr => s"& ($constr)")}"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}: [ ...int ${constraints
                    .fold("")(constr => s"& ($constr)")}]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}?: null | (int ${constraints
                    .fold("")(constr => s"& ($constr)")})"
              )
          end match
        case (attributeName, LongType(mode, constraints)) =>
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}!: int ${constraints
                    .fold("")(constr => s"& ($constr)")}"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}: [ ...int ${constraints
                    .fold("")(constr => s"& ($constr)")}]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}?: null | (int ${constraints
                    .fold("")(constr => s"& ($constr)")})"
              )
          end match
        case (attributeName, FloatType(mode, constraints)) =>
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}!: float ${constraints
                    .fold("")(constr => s"& ($constr)")}"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}: [ ...float ${constraints
                    .fold("")(constr => s"& ($constr)")}]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}?: null | (float ${constraints
                    .fold("")(constr => s"| ($constr)")})"
              )
          end match
        case (attributeName, DoubleType(mode, constraints)) =>
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}!: float ${constraints
                    .fold("")(constr => s"& ($constr)")}"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}: [ ...float ${constraints
                    .fold("")(constr => s"& ($constr)")}]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}?: null | (float ${constraints
                    .fold("")(constr => s"& ($constr)")})"
              )
          end match
        case (attributeName, BooleanType(mode, constraints)) =>
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}!: bool ${constraints
                    .fold("")(constr => s"& ($constr)")}"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}: [ ...bool ${constraints
                    .fold("")(constr => s"& ($constr)")}]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}?: null | (bool ${constraints
                    .fold("")(constr => s"& ($constr)")})"
              )
          end match
        case (attributeName, SqlDecimal(mode, constraints)) =>
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}!: number ${constraints
                    .fold("")(constr => s"& ($constr)")}"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}: [ ...number ${constraints
                    .fold("")(constr => s"& ($constr)")}]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}?: null | (number ${constraints
                    .fold("")(constr => s"& ($constr)")})"
              )
          end match
        case (attributeName, DateType(mode, constraints)) =>
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}!: string ${constraints
                    .fold("")(constr => s"& ($constr)")}"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}: [ ...string ${constraints
                    .fold("")(constr => s"& ($constr)")}]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}?: null | (string ${constraints
                    .fold("")(constr => s"& ($constr)")})"
              )
          end match
        case (attributeName, TimestampDataType(mode, constraints)) =>
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}!: string ${constraints
                    .fold("")(constr => s"& ($constr)")}"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}: [ ...string ${constraints
                    .fold("")(constr => s"& ($constr)")}]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}?: null | (string ${constraints
                    .fold("")(constr => s"& ($constr)")})"
              )
          end match
        case (attributeName, JsonType(mode)) =>
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}!: string"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}: [ ...string]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}?: string"
              )
          end match
        case (attributeName, str @ StructType(_, mode)) =>
          given n: Int = currentIndentation
          mode match
            case Mode.Required =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}!: {\n${internalGenerateCueModel(str, n + 2)}}"
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}: [ ...{\n${internalGenerateCueModel(str, n + 2)}$blanks}]"
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"$blanks${sanitezeAttributName(attributeName)}?: {\n${internalGenerateCueModel(str, n + 2)}}"
              )
          end match
      end match
      stringBuilder.append('\n')
    )
    stringBuilder.result()
  end internalGenerateCueModel

  internalGenerateCueModel(schema: StructType, 0)
end generateCueModel
