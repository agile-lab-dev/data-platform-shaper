package it.agilelab.dataplatformshaper.domain.model.schema

import cats.*
import cats.implicits.*
import io.circe.{ACursor, Json, ParsingFailure}
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*
import it.agilelab.dataplatformshaper.domain.model.schema.parsing.FoldingPhase

import java.time.{LocalDate, ZonedDateTime}
import scala.language.{dynamics, postfixOps}
import scala.util.{Failure, Success, Try}

package parsing {
  enum FoldingPhase:
    case FoldingPrimitive
    case BeginFoldingStruct
    case EndFoldingStruct
  end FoldingPhase
}

import it.agilelab.dataplatformshaper.domain.model.schema.parsing.FoldingPhase.*

implicit val jsonOrdering: Ordering[Json] = Ordering.by(_.toString())

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.asInstanceOf",
    "scalafix:DisableSyntax.isInstanceOf"
  )
)
private def unfoldPrimitive(
    name: String,
    value: Any,
    dataType: DataType,
    currentPath: String,
    func: (String, DataType, Any, FoldingPhase) => Unit
): Either[String, Unit] =
  dataType match
    case tpe @ StringType(mode, _) =>
      value match
        case value: String if mode === Required =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case value if value.isInstanceOf[List[?]] && mode === Repeated =>
          try
            value
              .asInstanceOf[List[String]]
              .sorted
              .foreach(str =>
                unfoldPrimitive(
                  name,
                  str,
                  tpe.copy(mode = Required),
                  currentPath,
                  func
                )
              )
            Right[String, Unit](())
          catch
            case _: Throwable =>
              Left[String, Unit](s"$value is not a List[string]")
        case value @ Some(_: String) if mode === Nullable =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case None if mode === Nullable =>
          func(currentPath, tpe, None: Option[String], FoldingPrimitive)
          Right[String, Unit](())
        case wrong =>
          Left[String, Unit](s"$wrong is not a string")
      end match
    case tpe @ IntType(mode, _) =>
      value match
        case value: Int if mode === Required =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case value if value.isInstanceOf[List[?]] && mode === Repeated =>
          try
            value
              .asInstanceOf[List[Int]]
              .sorted
              .foreach(int =>
                unfoldPrimitive(
                  name,
                  int,
                  tpe.copy(mode = Required),
                  currentPath,
                  func
                )
              )
            Right[String, Unit](())
          catch
            case _: Throwable =>
              Left[String, Unit](s"$value is not a List[Int]")
        case value @ Some(_: Int) if mode === Nullable =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case None if mode === Nullable =>
          func(currentPath, tpe, None: Option[Int], FoldingPrimitive)
          Right[String, Unit](())
        case wrong =>
          Left[String, Unit](s"$wrong is not an int")
      end match
    case tpe @ DateType(mode, _) =>
      value match
        case value: LocalDate if mode === Required =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case value if value.isInstanceOf[List[?]] && mode === Repeated =>
          try
            value
              .asInstanceOf[List[LocalDate]]
              .sorted
              .foreach(date =>
                unfoldPrimitive(
                  name,
                  date,
                  tpe.copy(mode = Required),
                  currentPath,
                  func
                )
              )
            Right[String, Unit](())
          catch
            case _: Throwable =>
              Left[String, Unit](s"$value is not a List[LocalDate]")
        case value @ Some(_: LocalDate) if mode === Nullable =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case None if mode === Nullable =>
          func(currentPath, tpe, None: Option[LocalDate], FoldingPrimitive)
          Right[String, Unit](())
        case wrong =>
          Left[String, Unit](s"$wrong is not a LocalDate")
      end match
    case tpe @ JsonType(mode) =>
      value match
        case value: Json if mode === Required =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case value if value.isInstanceOf[List[?]] && mode === Repeated =>
          try
            value
              .asInstanceOf[List[Json]]
              .sorted
              .foreach(date =>
                unfoldPrimitive(
                  name,
                  date,
                  tpe.copy(mode = Required),
                  currentPath,
                  func
                )
              )
            Right[String, Unit](())
          catch
            case _: Throwable =>
              Left[String, Unit](s"$value is not a List[Json]")
        case value @ Some(_: Json) if mode === Nullable =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case None if mode === Nullable =>
          func(currentPath, tpe, None: Option[Json], FoldingPrimitive)
          Right[String, Unit](())
        case wrong =>
          Left[String, Unit](s"$wrong is not a Json")
      end match
    case tpe @ TimestampType(mode, _) =>
      value match
        case value: ZonedDateTime if mode === Required =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case value if value.isInstanceOf[List[?]] && mode === Repeated =>
          try
            value
              .asInstanceOf[List[ZonedDateTime]]
              .sorted
              .foreach(timestamp =>
                unfoldPrimitive(
                  name,
                  timestamp,
                  tpe.copy(mode = Required),
                  currentPath,
                  func
                )
              )
            Right[String, Unit](())
          catch
            case _: Throwable =>
              Left[String, Unit](s"$value is not a List[ZonedDateTime]")
        case value @ Some(_: ZonedDateTime) if mode === Nullable =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case None if mode === Nullable =>
          func(currentPath, tpe, None: Option[ZonedDateTime], FoldingPrimitive)
          Right[String, Unit](())
        case wrong =>
          Left[String, Unit](s"$wrong is not a ZonedDateTime")
    case tpe @ DoubleType(mode, _) =>
      value match
        case value: Double if mode === Required =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case value if value.isInstanceOf[List[?]] && mode === Repeated =>
          try
            value
              .asInstanceOf[List[Double]]
              .sorted
              .foreach(num =>
                unfoldPrimitive(
                  name,
                  num,
                  tpe.copy(mode = Required),
                  currentPath,
                  func
                )
              )
            Right[String, Unit](())
          catch
            case _: Throwable =>
              Left[String, Unit](s"$value is not a List[Double]")
        case value @ Some(_: Double) if mode === Nullable =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case None if mode === Nullable =>
          func(currentPath, tpe, None: Option[Double], FoldingPrimitive)
          Right[String, Unit](())
        case wrong =>
          Left[String, Unit](s"$wrong is not a Double")
      end match
    case tpe @ FloatType(mode, _) =>
      value match
        case value: Float if mode === Required =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case value if value.isInstanceOf[List[?]] && mode === Repeated =>
          try
            value
              .asInstanceOf[List[Float]]
              .sorted
              .foreach(float =>
                unfoldPrimitive(
                  name,
                  float,
                  tpe.copy(mode = Required),
                  currentPath,
                  func
                )
              )
            Right[String, Unit](())
          catch
            case _: Throwable =>
              Left[String, Unit](s"$value is not a List[Float]")
        case value @ Some(_: Float) if mode === Nullable =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case None if mode === Nullable =>
          func(currentPath, tpe, None: Option[Float], FoldingPrimitive)
          Right[String, Unit](())
        case wrong =>
          Left[String, Unit](s"$wrong is not a float")
      end match
    case tpe @ LongType(mode, _) =>
      value match
        case value: Long if mode === Required =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case value if value.isInstanceOf[List[?]] && mode === Repeated =>
          try
            value
              .asInstanceOf[List[Long]]
              .sorted
              .foreach(long =>
                unfoldPrimitive(
                  name,
                  long,
                  tpe.copy(mode = Required),
                  currentPath,
                  func
                )
              )
            Right[String, Unit](())
          catch
            case _: Throwable =>
              Left[String, Unit](s"$value is not a List[Long]")
        case value @ Some(_: Long) if mode === Nullable =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case None if mode === Nullable =>
          func(currentPath, tpe, None: Option[Long], FoldingPrimitive)
          Right[String, Unit](())
        case wrong =>
          Left[String, Unit](s"$wrong is not a long")
      end match
    case tpe @ BooleanType(mode, _) =>
      value match
        case value: Boolean if mode === Required =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case value if value.isInstanceOf[List[?]] && mode === Repeated =>
          try
            value
              .asInstanceOf[List[Boolean]]
              .sorted
              .foreach(boolean =>
                unfoldPrimitive(
                  name,
                  boolean,
                  tpe.copy(mode = Required),
                  currentPath,
                  func
                )
              )
            Right[String, Unit](())
          catch
            case _: Throwable =>
              Left[String, Unit](s"$value is not a List[Boolean]")
        case value @ Some(_: Boolean) if mode === Nullable =>
          func(currentPath, tpe, value, FoldingPrimitive)
          Right[String, Unit](())
        case None if mode === Nullable =>
          func(currentPath, tpe, None: Option[Boolean], FoldingPrimitive)
          Right[String, Unit](())
        case wrong =>
          Left[String, Unit](s"$wrong is not a boolean")
      end match
    case wrong =>
      Left[String, Unit](s"$wrong type is unknown")
  end match
end unfoldPrimitive

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.isInstanceOf",
    "scalafix:DisableSyntax.asInstanceOf",
    "scalafix:DisableSyntax.var",
    "scalafix:DisableSyntax.=="
  )
)
private def unfoldStruct(
    name: String,
    tuple: Any,
    schema: DataType,
    currentPath: String,
    func: (String, DataType, Any, FoldingPhase) => Unit
): Either[String, Unit] =
  schema match
    case tpe @ StructType(records, mode) =>
      tuple match
        case value: Tuple if mode === Required =>
          val tuples =
            try Success(value.toArray.map(_.asInstanceOf[(String, Any)]).toMap)
            catch ex => Failure[Map[String, Any]](ex)
          tuples match
            case Success(tuples) =>
              val tupleFieldNames = tuples.map(_(0)).toSet
              val recordFieldNames = records.map(_(0)).toSet
              if tupleFieldNames === recordFieldNames then
                if tuples.size == records.length then
                  func(currentPath, tpe, tuple, BeginFoldingStruct)
                  var currentRes: Either[String, Unit] = Right[String, Unit](())
                  val managedRecords =
                    records.takeWhile(record =>
                      currentRes = unfoldDataType(
                        (record(0), tuples(record(0))),
                        record(1),
                        currentPath,
                        func
                      )
                      currentRes.isRight
                    )
                  func(currentPath, tpe, tuple, EndFoldingStruct)
                  if managedRecords.lengthIs == records.length then
                    Right[String, Unit](())
                  else currentRes
                  end if
                else
                  Left[String, Unit](
                    s"struct $name has wrong number of field, it's ${tuples.size}, but it should be ${records.length}"
                  )
                end if
              else
                Left[String, Unit](
                  s"struct $name has wrong list of field names $tupleFieldNames, it should be $recordFieldNames"
                )
              end if
            case Failure(_) =>
              Left[String, Unit](s"$value is not a conform tuple")
        case value if mode === Repeated && value.isInstanceOf[List[?]] =>
          val tuples: Try[List[Tuple]] =
            try Success(value.asInstanceOf[List[Tuple]])
            catch ex => Failure[List[Tuple]](ex)
          tuples match
            case Failure(_) =>
              Left[String, Unit](s"$value is not a conform tuple")
            case Success(tuples) =>
              var currentRes: Either[String, Unit] = Right[String, Unit](())
              tuples.takeWhile(tuple =>
                currentRes = unfoldStruct(
                  name,
                  tuple,
                  tpe.copy(mode = Required),
                  currentPath,
                  func
                )
                currentRes.isRight
              )
              currentRes
          end match
        case value if mode === Nullable && value.isInstanceOf[Option[?]] =>
          val tuples: Try[Option[Tuple]] =
            try Success(value.asInstanceOf[Option[Tuple]])
            catch ex => Failure[Option[Tuple]](ex)
          tuples match
            case Failure(_) =>
              Left[String, Unit](s"$value is not a conform tuple")
            case Success(tuples) =>
              tuples.map(tuple =>
                unfoldStruct(
                  name,
                  tuple,
                  tpe.copy(mode = Required),
                  currentPath,
                  func
                )
              ) match
                case Some(Right(_)) =>
                  Right[String, Unit](())
                case Some(Left(err)) =>
                  Left[String, Unit](err)
                case None =>
                  Right[String, Unit](())
              end match
          end match
        case wrong =>
          Left[String, Unit](s"$wrong is not a conform tuple")
      end match
    case wrong =>
      Left[String, Unit](s"$wrong is not a struct")
  end match
end unfoldStruct

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.asInstanceOf"
  )
)
private def unfoldDataType(
    tuple: Tuple,
    schema: DataType,
    currentPath: String,
    func: (String, DataType, Any, FoldingPhase) => Unit
): Either[String, Unit] =
  val tuple2 = tuple.asInstanceOf[Tuple2[String, Any]]
  val name = tuple2(0)
  val cp = if currentPath === "" then name else s"$currentPath/$name"
  schema match
    case tpe @ StructType(_, _) =>
      unfoldStruct(name, tuple2(1), tpe, s"$cp", func)
    case tpe @ StringType(_, _) =>
      unfoldPrimitive(name, tuple2(1), tpe, s"$cp", func)
    case tpe @ IntType(_, _) =>
      unfoldPrimitive(name, tuple2(1), tpe, s"$cp", func)
    case tpe @ DateType(_, _) =>
      unfoldPrimitive(name, tuple2(1), tpe, s"$cp", func)
    case tpe @ JsonType(_) =>
      unfoldPrimitive(name, tuple2(1), tpe, s"$cp", func)
    case tpe @ TimestampType(_, _) =>
      unfoldPrimitive(name, tuple2(1), tpe, s"$cp", func)
    case tpe @ DoubleType(_, _) =>
      unfoldPrimitive(name, tuple2(1), tpe, s"$cp", func)
    case tpe @ FloatType(_, _) =>
      unfoldPrimitive(name, tuple2(1), tpe, s"$cp", func)
    case tpe @ LongType(_, _) =>
      unfoldPrimitive(name, tuple2(1), tpe, s"$cp", func)
    case tpe @ BooleanType(_, _) =>
      unfoldPrimitive(name, tuple2(1), tpe, s"$cp", func)
    case wrong =>
      Left[String, Unit](s"$wrong type is unknown")
  end match
end unfoldDataType

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.asInstanceOf",
    "scalafix:DisableSyntax.var",
    "scalafix:DisableSyntax.=="
  )
)
def unfoldTuple(
    tuple: Tuple,
    schema: Schema,
    func: (String, DataType, Any, FoldingPhase) => Unit
): Either[String, Unit] =
  val tuples =
    try Success(tuple.toArray.map(_.asInstanceOf[(String, Any)]).toMap)
    catch case ex => Failure[Map[String, Any]](ex)
  tuples match
    case Success(tuples) =>
      val tupleFieldNames = tuples.map(_(0)).toSet
      val recordFieldNames = schema.records.map(_(0)).toSet
      if tupleFieldNames === recordFieldNames then
        if tuples.size == schema.records.length then
          var currentRes: Either[String, Unit] = Right[String, Unit](())
          val managedRecords = schema.records.takeWhile(record =>
            val tuple = (record(0), tuples(record(0))).asInstanceOf[Tuple]
            val dataType = record(1)
            currentRes = unfoldDataType(tuple, dataType, "", func)
            currentRes.isRight
          )
          if managedRecords.lengthIs == schema.records.length then
            Right[String, Unit](())
          else currentRes
          end if
        else
          Left[String, Unit](
            s"input tuple has wrong number of field, it's ${tuples.size}, but it should be ${schema.records.length}"
          )
        end if
      else
        Left[String, Unit](
          s"input tuple has wrong list of field names $tupleFieldNames, it should be $recordFieldNames"
        )
      end if
    case Failure(_) =>
      Left[String, Unit](s"$tuple is not a conform tuple")
end unfoldTuple

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.throw",
    "scalafix:DisableSyntax.var"
  )
)
@throws[IllegalArgumentException]
def jsonToTupleChecked(
    json: Json,
    schema: Schema
): Tuple =
  var tuple: Tuple = EmptyTuple
  schema.records.reverse.foreach(pair =>
    tuple = (
      pair(0), {
        val obj: ACursor = json.hcursor.downField(pair(0))
        pair(1) match
          case IntType(mode, _) =>
            mode match
              case Required =>
                obj
                  .as[Int]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Repeated =>
                obj
                  .as[List[Int]]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Nullable =>
                obj.as[Int].toOption
            end match
          case DateType(mode, _) =>
            mode match
              case Required =>
                obj
                  .as[LocalDate]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Repeated =>
                obj
                  .as[List[LocalDate]]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Nullable =>
                obj.as[LocalDate].toOption
            end match
          case JsonType(mode) =>
            mode match
              case Required =>
                obj
                  .as[Json]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Repeated =>
                obj
                  .as[List[Json]]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Nullable =>
                obj.as[Json].toOption
            end match
          case TimestampType(mode, _) =>
            mode match
              case Required =>
                obj
                  .as[ZonedDateTime]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Repeated =>
                obj
                  .as[List[ZonedDateTime]]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Nullable =>
                obj.as[ZonedDateTime].toOption
            end match
          case DoubleType(mode, _) =>
            mode match
              case Required =>
                obj
                  .as[Double]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Repeated =>
                obj
                  .as[List[Double]]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Nullable =>
                obj.as[Double].toOption
            end match
          case FloatType(mode, _) =>
            mode match
              case Required =>
                obj
                  .as[Float]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Repeated =>
                obj
                  .as[List[Float]]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Nullable =>
                obj.as[Float].toOption
            end match
          case LongType(mode, _) =>
            mode match
              case Required =>
                obj
                  .as[Long]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Repeated =>
                obj
                  .as[List[Long]]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Nullable =>
                obj.as[Long].toOption
            end match
          case BooleanType(mode, _) =>
            mode match
              case Required =>
                obj
                  .as[Boolean]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Repeated =>
                obj
                  .as[List[Boolean]]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Nullable =>
                obj.as[Boolean].toOption
            end match
          case StringType(mode, _) =>
            mode match
              case Required =>
                obj
                  .as[String]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Repeated =>
                obj
                  .as[List[String]]
                  .toOption
                  .getOrElse(throw new IllegalArgumentException(s"Wrong value"))
              case Nullable =>
                obj.as[String].toOption
            end match
          case schema @ StructType(_, mode) =>
            mode match
              case Required =>
                jsonToTupleChecked(obj.focus.get, schema)
              case Nullable =>
                obj.focus.fold(None)(json =>
                  Some(jsonToTupleChecked(json, schema))
                )
              case Repeated =>
                obj.focus
                  .flatMap(_.asArray)
                  .get
                  .map(json =>
                    jsonToTupleChecked(json, schema.copy(mode = Required))
                  )
                  .toList
            end match
          case tpe =>
            throw new IllegalArgumentException(s"$tpe is not supported")
        end match
      }
    ) *: tuple
  )
  tuple
end jsonToTupleChecked

@SuppressWarnings(
  Array(
    "scalafix:DisableSyntax.throw",
    "scalafix:DisableSyntax.asInstanceOf"
  )
)
@throws[IllegalArgumentException]
def tupleToJsonChecked(
    tuple: Tuple,
    schema: Schema
): Json =
  val tupleFields = tuple.toArray.map(_.asInstanceOf[(String, Any)]).toMap
  Json.fromFields(
    schema.records.reverse.map(pair =>
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
                  Json.fromInt(tupleFieldValue.asInstanceOf[Int])
                case Repeated =>
                  Json.fromValues(
                    tupleFieldValue
                      .asInstanceOf[List[Int]]
                      .sorted
                      .map(Json.fromInt)
                  )
                case Nullable =>
                  tupleFieldValue
                    .asInstanceOf[Option[Int]]
                    .fold(Json.Null)(Json.fromInt)
              end match
            case DateType(mode, _) =>
              mode match
                case Required =>
                  Json.fromString(
                    tupleFieldValue.toString
                      .asInstanceOf[String]
                  )
                case Repeated =>
                  Json.fromValues(
                    tupleFieldValue
                      .asInstanceOf[List[LocalDate]]
                      .sorted
                      .map(date => date.toString)
                      .asInstanceOf[List[String]]
                      .map(Json.fromString)
                  )
                case Nullable =>
                  tupleFieldValue
                    .asInstanceOf[Option[LocalDate]]
                    .fold(Json.Null)(date => Json.fromString(date.toString))
              end match
            case JsonType(mode) =>
              mode match
                case Required =>
                  Json.fromString(
                    tupleFieldValue.toString
                      .asInstanceOf[String]
                  )
                case Repeated =>
                  Json.fromValues(
                    tupleFieldValue
                      .asInstanceOf[List[Json]]
                      .sorted
                      .map(json => json.toString)
                      .asInstanceOf[List[String]]
                      .map(Json.fromString)
                  )
                case Nullable =>
                  tupleFieldValue
                    .asInstanceOf[Option[Json]]
                    .fold(Json.Null)(date => Json.fromString(date.toString))
              end match
            case TimestampType(mode, _) =>
              mode match
                case Required =>
                  Json.fromString(
                    tupleFieldValue.toString
                      .asInstanceOf[String]
                  )
                case Repeated =>
                  Json.fromValues(
                    tupleFieldValue
                      .asInstanceOf[List[ZonedDateTime]]
                      .sorted
                      .map(ts => ts.toString)
                      .asInstanceOf[List[String]]
                      .map(Json.fromString)
                  )
                case Nullable =>
                  tupleFieldValue
                    .asInstanceOf[Option[ZonedDateTime]]
                    .fold(Json.Null)(date => Json.fromString(date.toString))
              end match
            case DoubleType(mode, _) =>
              mode match
                case Required =>
                  Json
                    .fromDouble(tupleFieldValue.asInstanceOf[Double])
                    .getOrElse(
                      throw new IllegalArgumentException(s"Wrong value")
                    )
                case Repeated =>
                  Json.fromValues(
                    tupleFieldValue
                      .asInstanceOf[List[Double]]
                      .sorted
                      .map(el =>
                        Json
                          .fromDouble(el)
                          .getOrElse(
                            throw new IllegalArgumentException(s"Wrong value")
                          )
                      )
                  )
                case Nullable =>
                  tupleFieldValue
                    .asInstanceOf[Option[Double]]
                    .fold(Json.Null)(el =>
                      Json
                        .fromDouble(el)
                        .getOrElse(
                          throw new IllegalArgumentException(s"Wrong value")
                        )
                    )
              end match
            case FloatType(mode, _) =>
              mode match
                case Required =>
                  Json
                    .fromFloat(tupleFieldValue.asInstanceOf[Float])
                    .getOrElse(
                      throw new IllegalArgumentException(s"Wrong value")
                    )
                case Repeated =>
                  Json.fromValues(
                    tupleFieldValue
                      .asInstanceOf[List[Float]]
                      .sorted
                      .map(el =>
                        Json
                          .fromFloat(el)
                          .getOrElse(
                            throw new IllegalArgumentException(s"Wrong value")
                          )
                      )
                  )
                case Nullable =>
                  tupleFieldValue
                    .asInstanceOf[Option[Float]]
                    .fold(Json.Null)(el =>
                      Json
                        .fromFloat(el)
                        .getOrElse(
                          throw new IllegalArgumentException(s"Wrong value")
                        )
                    )
              end match
            case LongType(mode, _) =>
              mode match
                case Required =>
                  Json.fromLong(tupleFieldValue.asInstanceOf[Long])
                case Repeated =>
                  Json.fromValues(
                    tupleFieldValue
                      .asInstanceOf[List[Long]]
                      .sorted
                      .map(Json.fromLong)
                  )
                case Nullable =>
                  tupleFieldValue
                    .asInstanceOf[Option[Long]]
                    .fold(Json.Null)(Json.fromLong)
              end match
            case BooleanType(mode, _) =>
              mode match
                case Required =>
                  Json.fromBoolean(tupleFieldValue.asInstanceOf[Boolean])
                case Repeated =>
                  Json.fromValues(
                    tupleFieldValue
                      .asInstanceOf[List[Boolean]]
                      .sorted
                      .map(Json.fromBoolean)
                  )
                case Nullable =>
                  tupleFieldValue
                    .asInstanceOf[Option[Boolean]]
                    .fold(Json.Null)(Json.fromBoolean)
              end match
            case StringType(mode, _) =>
              mode match
                case Required =>
                  Json.fromString(tupleFieldValue.asInstanceOf[String])
                case Repeated =>
                  Json.fromValues(
                    tupleFieldValue
                      .asInstanceOf[List[String]]
                      .sorted
                      .map(Json.fromString)
                  )
                case Nullable =>
                  tupleFieldValue
                    .asInstanceOf[Option[String]]
                    .fold(Json.Null)(Json.fromString)
              end match
            case schema @ StructType(_, mode) =>
              mode match
                case Required =>
                  tupleToJsonChecked(
                    tupleFieldValue.asInstanceOf[Tuple],
                    schema
                  )
                case Nullable =>
                  tupleFieldValue
                    .asInstanceOf[Option[Tuple]]
                    .fold(Json.Null)(tupleToJsonChecked(_, schema))
                case Repeated =>
                  Json.fromValues(
                    tupleFieldValue
                      .asInstanceOf[List[Tuple]]
                      .map(tuple =>
                        tupleToJsonChecked(tuple, schema.copy(mode = Required))
                      )
                  )
              end match
            case tpe =>
              throw new IllegalArgumentException(s"$tpe is not supported")
          end match
        }
      )
    )
  )
end tupleToJsonChecked

def jsonToTuple(
    json: Json,
    schema: Schema
): Either[ParsingFailure, Tuple] =
  Try(
    jsonToTupleChecked(json, schema)
  ) match
    case Failure(ex) =>
      Left(
        ParsingFailure(
          s"the provided json document is not conform to the provided schema",
          ex
        )
      )
    case Success(tuple) =>
      Right(tuple)
  end match
end jsonToTuple

def tupleToJson(
    tuple: Tuple,
    schema: Schema
): Either[ParsingFailure, Json] =
  Try(
    tupleToJsonChecked(tuple, schema)
  ) match
    case Failure(ex) =>
      Left(
        ParsingFailure(
          s"the provided json document is not conform to the provided schema",
          ex
        )
      )
    case Success(json) =>
      Right(json)
  end match
end tupleToJson
