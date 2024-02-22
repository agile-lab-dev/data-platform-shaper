package it.agilelab.dataplatformshaper.domain.model.schema

import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.{
  Nullable,
  Repeated,
  Required
}
import jakarta.el.{ELProcessor, ExpressionFactory}
import org.glassfish.expressly.ExpressionFactoryImpl

import scala.reflect.ClassTag
import scala.util.Try

def tupleToMappedTuple(
    sourceTuple: Tuple,
    sourceTupleSchema: Schema,
    mappingTuple: Tuple,
    mappedTupleSchema: Schema
): Either[String, Tuple] =
  val elProcesspr = ELProcessor()
  val exFactory = ExpressionFactoryImpl()
  for {
    _ <- validateMappingTuple(mappingTuple, mappedTupleSchema)
    _ <- {
      val pt = parseTuple(sourceTuple, sourceTupleSchema)
      elProcesspr.defineBean("instance", sourceTuple: DynamicTuple)
      pt
    }
    mt <- Try(
      tupleToMappedTupleChecked(
        elProcesspr,
        exFactory,
        mappingTuple,
        mappedTupleSchema,
        false
      )
    ).toEither.leftMap(_.getMessage)
  } yield mt
end tupleToMappedTuple

@inline def bracket(fieldExpr: String) = s"""$${ $fieldExpr }"""

@inline private def evalField[T](
    fieldExpr: String
)(using elp: ELProcessor, ef: ExpressionFactory, onlyValidation: Boolean): Any =
  if onlyValidation then
    ef.createValueExpression(
      elp.getELManager.getELContext,
      bracket(fieldExpr),
      classOf[AnyRef]
    )
    ()
  else elp.eval[T](fieldExpr)
  end if
end evalField

@inline private def evalRepeatedField[T](fieldExpr: List[String])(using
    elp: ELProcessor,
    ef: ExpressionFactory,
    onlyValidation: Boolean
): List[Any] =
  if onlyValidation then
    fieldExpr.foreach(expr =>
      ef.createValueExpression(
        elp.getELManager.getELContext,
        bracket(expr),
        classOf[AnyRef]
      )
    )
    List.empty[Unit]
  else fieldExpr.map(value => elp.eval[T](value))
end evalRepeatedField

@inline private def evalOptionalField[T](fieldExpr: Option[String])(using
    elp: ELProcessor,
    ef: ExpressionFactory,
    onlyValidation: Boolean
): Option[Any] =
  if onlyValidation then
    fieldExpr.fold(None)(expr =>
      ef.createValueExpression(
        elp.getELManager.getELContext,
        bracket(expr),
        classOf[AnyRef]
      )
    )
    Option.empty[Unit]
  else fieldExpr.fold(None)(value => Some(elp.eval[T](value)))
end evalOptionalField

def validateMappingTuple(
    mappingTuple: Tuple,
    mappedTupleSchema: Schema
): Either[String, Unit] =
  Try(
    tupleToMappedTupleChecked(
      ELProcessor(),
      ExpressionFactoryImpl(),
      mappingTuple,
      mappedTupleSchema,
      true
    )
  ).toEither
    .leftMap(t => s"The mapping tuple: $mappingTuple is wrong: ${t.getMessage}")
    .map(_ => ())
end validateMappingTuple

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.throw",
    "scalafix:DisableSyntax.asInstanceOf"
  )
)
@throws[IllegalArgumentException]
def tupleToMappedTupleChecked(
    elProcessor: ELProcessor,
    exFactory: ExpressionFactory,
    mappingTuple: Tuple,
    mappedTupleSchema: Schema,
    onlyValidation: Boolean
): Tuple =
  given elp: ELProcessor = elProcessor
  given exf: ExpressionFactory = exFactory
  given ov: Boolean = onlyValidation
  val tupleFields =
    mappingTuple.toArray.map(_.asInstanceOf[(String, Any)]).toMap
  Tuple.fromArray(
    mappedTupleSchema.records
      .map(pair =>
        (
          pair(0), {
            val tupleFieldValue: Any = tupleFields.getOrElse(
              pair(0),
              throw IllegalArgumentException(
                s"No field with this name: ${pair(0)}"
              )
            )
            pair(1) match
              case IntType(mode, _) =>
                mode match
                  case Required =>
                    evalField[Int](tupleFieldValue.asInstanceOf[String])
                  case Repeated =>
                    evalRepeatedField[Int](
                      tupleFieldValue.asInstanceOf[List[String]]
                    )
                  case Nullable =>
                    evalOptionalField(
                      tupleFieldValue.asInstanceOf[Option[String]]
                    )
                end match
              case LongType(mode, _) =>
                mode match
                  case Required =>
                    evalField[Long](tupleFieldValue.asInstanceOf[String])
                  case Repeated =>
                    evalRepeatedField[Long](
                      tupleFieldValue.asInstanceOf[List[String]]
                    )
                  case Nullable =>
                    evalOptionalField[Long](
                      tupleFieldValue.asInstanceOf[Option[String]]
                    )
                end match
              case FloatType(mode, _) =>
                mode match
                  case Required =>
                    evalField[Float](tupleFieldValue.asInstanceOf[String])
                  case Repeated =>
                    evalRepeatedField[Float](
                      tupleFieldValue.asInstanceOf[List[String]]
                    )
                  case Nullable =>
                    evalOptionalField[Float](
                      tupleFieldValue.asInstanceOf[Option[String]]
                    )
                end match
              case DoubleType(mode, _) =>
                mode match
                  case Required =>
                    evalField[Double](tupleFieldValue.asInstanceOf[String])
                  case Repeated =>
                    evalRepeatedField[Double](
                      tupleFieldValue.asInstanceOf[List[String]]
                    )
                  case Nullable =>
                    evalOptionalField[Double](
                      tupleFieldValue.asInstanceOf[Option[String]]
                    )
                end match
              case StringType(mode, _) =>
                mode match
                  case Required =>
                    evalField[String](tupleFieldValue.asInstanceOf[String])
                  case Repeated =>
                    evalRepeatedField[String](
                      tupleFieldValue.asInstanceOf[List[String]]
                    )
                  case Nullable =>
                    evalOptionalField[String](
                      tupleFieldValue.asInstanceOf[Option[String]]
                    )
                end match
              case schema @ StructType(_, mode) =>
                mode match
                  case Required =>
                    tupleToMappedTupleChecked(
                      elProcessor,
                      exFactory,
                      tupleFieldValue.asInstanceOf[Tuple],
                      schema,
                      onlyValidation
                    )
                  case Nullable =>
                    tupleFieldValue
                      .asInstanceOf[Option[Tuple]]
                      .fold(None)(
                        tupleToMappedTupleChecked(
                          elProcessor,
                          exFactory,
                          _,
                          schema,
                          onlyValidation
                        )
                      )
                  case Repeated =>
                    tupleFieldValue
                      .asInstanceOf[List[Tuple]]
                      .map(tuple =>
                        tupleToMappedTupleChecked(
                          elProcessor,
                          exFactory,
                          tuple,
                          schema.copy(mode = Required),
                          onlyValidation
                        )
                      )
                end match
              case tpe =>
                throw IllegalArgumentException(s"$tpe is not supported")
            end match
          }
        )
      )
      .toArray
  )
end tupleToMappedTupleChecked
