package it.agilelab.dataplatformshaper.domain.model.schema

import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.{
  Nullable,
  Repeated,
  Required
}
import jakarta.el.ELProcessor

import scala.util.Try

def tupleToMappedTuple(
    sourceTuple: Tuple,
    sourceTupleSchema: Schema,
    mappingTuple: Tuple,
    mappedTupleSchema: Schema
): Either[String, Tuple] =
  val elProcesspr = new ELProcessor()

  for {
    _ <- {
      val pt = parseTuple(sourceTuple, sourceTupleSchema)
      elProcesspr.defineBean("instance", sourceTuple: DynamicTuple)
      pt
    }
    mt <- Try(
      tupleToMappedTupleChecked(elProcesspr, mappingTuple, mappedTupleSchema)
    ).toEither.leftMap(_.getMessage)
  } yield mt
end tupleToMappedTuple

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.throw",
    "scalafix:DisableSyntax.asInstanceOf"
  )
)
@throws[IllegalArgumentException]
def tupleToMappedTupleChecked(
    elProcessor: ELProcessor,
    mappingTuple: Tuple,
    mappedTupleSchema: Schema
): Tuple =
  val tupleFields =
    mappingTuple.toArray.map(_.asInstanceOf[(String, Any)]).toMap
  Tuple.fromArray(
    mappedTupleSchema.records
      .map(pair =>
        (
          pair(0), {
            val tupleFieldValue: Any = tupleFields.getOrElse(
              pair(0),
              throw new IllegalArgumentException(s"Wrong value")
            )
            pair(1) match
              case IntType(mode, _) =>
                mode match
                  case Required =>
                    elProcessor.eval[Int](tupleFieldValue.asInstanceOf[String])
                  case Repeated =>
                    tupleFieldValue
                      .asInstanceOf[List[Int]]
                  case Nullable =>
                    tupleFieldValue
                      .asInstanceOf[Option[Int]]
                end match
              case LongType(mode, _) =>
                mode match
                  case Required =>
                    elProcessor.eval[Long](tupleFieldValue.asInstanceOf[String])
                  case Repeated =>
                    tupleFieldValue
                      .asInstanceOf[List[String]]
                      .map(value => elProcessor.eval[Long](value))
                  case Nullable =>
                    tupleFieldValue
                      .asInstanceOf[Option[String]]
                      .fold(None)(value => elProcessor.eval(value))
                end match
              case StringType(mode, _) =>
                mode match
                  case Required =>
                    elProcessor
                      .eval[String](tupleFieldValue.asInstanceOf[String])
                  case Repeated =>
                    tupleFieldValue
                      .asInstanceOf[List[String]]
                  case Nullable =>
                    tupleFieldValue
                      .asInstanceOf[Option[String]]
                end match
              case schema @ StructType(_, mode) =>
                mode match
                  case Required =>
                    tupleToMappedTupleChecked(
                      elProcessor,
                      tupleFieldValue.asInstanceOf[Tuple],
                      schema
                    )
                  case Nullable =>
                    tupleFieldValue
                      .asInstanceOf[Option[Tuple]]
                      .fold(None)(
                        tupleToMappedTupleChecked(elProcessor, _, schema)
                      )
                  case Repeated =>
                    tupleFieldValue
                      .asInstanceOf[List[Tuple]]
                      .map(tuple =>
                        tupleToMappedTupleChecked(
                          elProcessor,
                          tuple,
                          schema.copy(mode = Required)
                        )
                      )
                end match
              case tpe =>
                throw new IllegalArgumentException(s"$tpe is not supported")
            end match
          }
        )
      )
      .toArray
  )
end tupleToMappedTupleChecked
