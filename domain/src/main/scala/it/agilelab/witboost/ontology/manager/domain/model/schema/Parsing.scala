package it.agilelab.witboost.ontology.manager.domain.model.schema

import cats.*
import cats.implicits.*
import it.agilelab.witboost.ontology.manager.domain.model.schema.*
import it.agilelab.witboost.ontology.manager.domain.model.schema.Mode.*

import scala.language.{dynamics, postfixOps}
import scala.util.{Failure, Success, Try}

private def unfoldPrimitive(
    name: String,
    value: Any,
    dataType: DataType,
    currentPath: String,
    func: (String, DataType, Any) => Unit
): Either[String, Unit] =
  dataType match
    case tpe @ StringType(mode) =>
      value match
        case value: String if mode === Required =>
          func(currentPath, tpe, value)
          Right[String, Unit](())
        case value if value.isInstanceOf[List[_]] && mode === Repeated =>
          try
            value
              .asInstanceOf[List[String]]
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
          func(currentPath, tpe, value)
          Right[String, Unit](())
        case None if mode === Nullable =>
          func(currentPath, tpe, None: Option[String])
          Right[String, Unit](())
        case wrong =>
          Left[String, Unit](s"$wrong is not a string")
      end match
    case tpe @ IntType(mode) =>
      value match
        case value: Int if mode === Required =>
          func(currentPath, tpe, Some(value))
          Right[String, Unit](())
        case value if value.isInstanceOf[List[_]] && mode === Repeated =>
          try
            value
              .asInstanceOf[List[Int]]
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
          func(currentPath, tpe, value)
          Right[String, Unit](())
        case None if mode === Nullable =>
          func(currentPath, tpe, None: Option[Int])
          Right[String, Unit](())
        case wrong =>
          Left[String, Unit](s"$wrong is not an int")
      end match
    case wrong =>
      Left[String, Unit](s"$wrong type is unknown")
  end match
end unfoldPrimitive

private def unfoldStruct(
    name: String,
    tuple: Any,
    schema: DataType,
    currentPath: String,
    func: (String, DataType, Any) => Unit
): Either[String, Unit] =
  schema match
    case tpe @ StructType(records, mode) =>
      func(currentPath, tpe, tuple)
      tuple match
        case value: Tuple if mode === Required =>
          val tuples =
            try Success(value.toArray.map(_.asInstanceOf[(String, Any)]))
            catch ex => Failure[Array[(String, Any)]](ex)
          tuples match
            case Success(tuples) =>
              val tupleFieldNames = tuples.map(_(0)).toSet
              val recordFieldNames = records.map(_(0)).toSet
              if tupleFieldNames === recordFieldNames then
                val indexedRecords: List[((String, DataType), Int)] =
                  records.zipWithIndex
                if tuples.lengthIs == records.length then
                  var currentRes: Either[String, Unit] = Right[String, Unit](())
                  val managedRecords =
                    indexedRecords.takeWhile(recordWithIndex =>
                      currentRes = unfoldDataType(
                        tuples(recordWithIndex(1)),
                        recordWithIndex(0)(1),
                        currentPath,
                        (_: String, _: DataType, _: Any) => ()
                      )
                      currentRes.isRight
                    )
                  if managedRecords.lengthIs == records.length then
                    Right[String, Unit](())
                  else currentRes
                  end if
                else
                  Left[String, Unit](
                    s"struct $name has wrong number of field, it's ${tuples.lengthIs}, but it should be ${records.length}"
                  )
                end if
              else
                Left[String, Unit](
                  s"struct $name has wrong list of field names $tupleFieldNames, it should be $recordFieldNames"
                )
              end if
            case Failure(_) =>
              Left[String, Unit](s"$value is not a conform tuple")
        case value if mode === Repeated && value.isInstanceOf[List[_]] =>
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
        case value if mode === Nullable && value.isInstanceOf[Option[_]] =>
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

private def unfoldDataType(
    tuple: Tuple,
    schema: DataType,
    currentPath: String,
    func: (String, DataType, Any) => Unit
): Either[String, Unit] =
  val tuple2 = tuple.asInstanceOf[Tuple2[String, Any]]
  val name = tuple2(0)
  val cp = if currentPath === "" then name else s"$currentPath/$name"
  schema match
    case tpe @ StructType(_, _) =>
      unfoldStruct(name, tuple2(1), tpe, s"$cp", func)
    case tpe @ StringType(_) =>
      unfoldPrimitive(name, tuple2(1), tpe, s"$cp", func)
    case tpe @ IntType(_) =>
      unfoldPrimitive(name, tuple2(1), tpe, s"$cp", func)
    case wrong =>
      Left[String, Unit](s"$wrong type is unknown")
  end match
end unfoldDataType

def unfoldTuple(
    tuple: Tuple,
    schema: Schema,
    func: (String, DataType, Any) => Unit
): Either[String, Unit] =
  val tuples =
    try Success(tuple.toArray.map((_: Object).asInstanceOf[(String, Any)]))
    catch case ex => Failure[Array[(String, Any)]](ex)
  tuples match
    case Success(tuples) =>
      val tupleFieldNames = tuples.map(_(0)).toSet
      val recordFieldNames = schema.records.map(_(0)).toSet
      if tupleFieldNames === recordFieldNames then
        if tuples.lengthIs == schema.records.length then
          val indexedRecords = schema.records.zipWithIndex
          var currentRes: Either[String, Unit] = Right[String, Unit](())
          val managedRecords = indexedRecords.takeWhile(
            (recordWithIndex: ((String, DataType), Int)) =>
              val tuple: Tuple = tuples(recordWithIndex(1)).asInstanceOf[Tuple]
              val dataType = recordWithIndex(0)(1)
              currentRes = unfoldDataType(tuple, dataType, "", func)
              currentRes.isRight
          )
          if managedRecords.lengthIs == schema.records.length then
            Right[String, Unit](())
          else currentRes
          end if
        else
          Left[String, Unit](
            s"input tuple has wrong number of field, it's ${tuples.lengthIs}, but it should be ${schema.records.length}"
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
