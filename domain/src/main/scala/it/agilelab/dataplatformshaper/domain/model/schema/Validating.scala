package it.agilelab.dataplatformshaper.domain.model.schema

import io.circe.*
import io.circe.yaml.syntax.*

import java.io.{File, PrintWriter}
import scala.collection.mutable
import scala.sys.process.*
import scala.util.Using.Releasable
import scala.util.{Failure, Success, Using}

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
        case (attributeName, TimestampType(mode, constraints)) =>
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
                s"""$blanks${sanitizeAttributeName(
                    attributeName
                  )}!: {\n${internalGenerateCueModel(str, n + 2)}} //GEN"""
              )
            case Mode.Repeated =>
              stringBuilder.append(
                s"""$blanks${sanitizeAttributeName(
                    attributeName
                  )}: [ ...{\n${internalGenerateCueModel(
                    str,
                    n + 2
                  )}$blanks}] //GEN"""
              )
            case Mode.Nullable =>
              stringBuilder.append(
                s"""$blanks${sanitizeAttributeName(
                    attributeName
                  )}?: null | ({\n${internalGenerateCueModel(
                    str,
                    n + 2
                  )}}) //GEN"""
              )
          end match
      end match
      stringBuilder.append('\n')
    )
    stringBuilder.result()
  end internalGenerateCueModel

  internalGenerateCueModel(schema: StructType, 0)
end generateCueModel

given Releasable[File] with
  def release(file: File): Unit =
    val _ = file.delete()
  end release
end given

def cueValidateModel(
    schema: Schema
): Either[List[String], Unit] =

  Using.Manager { use =>
    val cueFile = use(File.createTempFile("test", ".cue"))
    val pw1 = use(new PrintWriter(cueFile))
    val model = generateCueModel(schema)
    pw1.write(model)
    pw1.flush()
    pw1.close()
    val errorsBuffer = List.newBuilder[String]
    s"cue eval ${cueFile.getAbsolutePath}" ! ProcessLogger(
      _ => (),
      errorsBuffer += _
    )
    val errors = errorsBuffer
      .result()
      .filter(error => !error.contains("test"))
      .map(err => err.substring(0, err.lastIndexOf(':')))
    if errors.isEmpty then Right(()) else Left(errors)
  } match
    case Failure(error) =>
      Left(
        List(
          s"Impossible to validate, the following error occurred during the model validation: ${error.getMessage}}"
        )
      )
    case Success(either) =>
      either
  end match
end cueValidateModel

def cueValidate(schema: Schema, values: Tuple): Either[List[String], Unit] =
  Using.Manager { use =>
    val yamlFile = use(File.createTempFile("test", ".yaml"))
    val cueFile = use(File.createTempFile("test", ".cue"))
    val pw1 = use(new PrintWriter(yamlFile))
    val pw2 = use(new PrintWriter(cueFile))
    val doc = tupleToJson(values, schema)
      .map(_.asYaml.spaces2)
      .toOption
      .get
    pw1.write(doc)
    pw1.flush()
    pw1.close()
    val model = generateCueModel(schema)
    pw2.write(model)
    pw2.flush()
    pw2.close()
    val errorsBuffer = List.newBuilder[String]
    s"cue eval ${yamlFile.getAbsolutePath} ${cueFile.getAbsolutePath} -c" ! ProcessLogger(
      _ => (),
      errorsBuffer += _
    )
    val errors = errorsBuffer
      .result()
      .filter(error => !error.contains("test"))
      .map(err => err.substring(0, err.lastIndexOf(':')))
    if errors.isEmpty then Right(()) else Left(errors)
  } match
    case Failure(error) =>
      Left(
        List(
          s"Impossible to validate, the following error occurred during the validation: ${error.getMessage}}"
        )
      )
    case Success(either) =>
      either
  end match
end cueValidate
