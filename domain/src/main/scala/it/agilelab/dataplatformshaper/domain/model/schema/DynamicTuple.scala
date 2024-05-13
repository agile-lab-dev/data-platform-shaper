package it.agilelab.dataplatformshaper.domain.model.schema

import cats.implicits.*
import it.agilelab.dataplatformshaper.domain.model.schema.Schema

import scala.Tuple.Union
import scala.annotation.{tailrec, unused}
import scala.language.{
  dynamics,
  implicitConversions,
  postfixOps,
  strictEquality
}
import scala.reflect.Typeable
import scala.util.Try

final case class DynamicTuple(tuple: Any) extends Dynamic:

  given CanEqual[Tuple, Tuple] = CanEqual.derived

  @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
  def selectDynamic(method: String): DynamicTuple =
    tuple match
      case t: Tuple =>
        val tuples = t.toList.asInstanceOf[List[(String, Any)]].toMap
        DynamicTuple(tuples(method))
      case t: Option[_] if method === "get" =>
        DynamicTuple(t.asInstanceOf[Option[Tuple]])
      case t: List[_] if method.forall(_.isDigit) =>
        DynamicTuple(t(method))
  end selectDynamic

  @SuppressWarnings(
    Array("scalafix:DisableSyntax.asInstanceOf", "scalafix:DisableSyntax.var")
  )
  def updateDynamic(method: String)(value: Any): DynamicTuple =
    tuple match
      case t: Tuple =>
        val tuples = t.toList.asInstanceOf[List[(String, Any)]]
        var tuple: Tuple = EmptyTuple
        tuples.foreach(p =>
          if p(0) === method then
            value match
              case DynamicTuple(tup) =>
                tuple = tuple :* (p._1, tup)
              case _ =>
                tuple = tuple :* (p._1, value)
          else tuple = tuple :* (p._1, p._2)
        )
        DynamicTuple(tuple)
      case t: List[_] =>
        val arr = t.toArray[Any]
        value match
          case DynamicTuple(tuple) =>
            arr.update(method, tuple)
          case _ =>
            arr.update(method, value)
        DynamicTuple(arr.toList)
      case _ =>
        this
  end updateDynamic

  @unused
  @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
  def applyDynamic(method: String)(arg: Int): DynamicTuple =
    val w = selectDynamic(method)
    DynamicTuple(w.tuple.asInstanceOf[List[Tuple]](arg))
  end applyDynamic

  def value[T: Typeable]: T =
    tuple match
      case t: T => t
    end match
  end value

  @SuppressWarnings(Array("scalafix:DisableSyntax.asInstanceOf"))
  def replace(path: String, value: Any): Either[String, DynamicTuple] =
    @tailrec
    def recursiveReplace(
      tuple: DynamicTuple,
      path: List[String],
      value: Any
    ): DynamicTuple =
      path match
        case head :: tail =>
          recursiveReplace(
            tuple,
            tail,
            tail.reverse
              .foldLeft(tuple)((tuple, field) => tuple.selectDynamic(field))
              .updateDynamic(head)(value)
          )
        case Nil =>
          value.asInstanceOf[DynamicTuple]
    end recursiveReplace
    Try(recursiveReplace(this, path.split("/").toList.reverse, value)).toEither
      .leftMap(t =>
        s"""Error '${t.getLocalizedMessage}' trying to modify the tuple with the path $path"""
      )
  end replace

  def getValue(path: String): Either[String, Any] =
    @inline
    def listPathGet(tuple: DynamicTuple, path: Array[String]): DynamicTuple =
      path.foldLeft(tuple)((tuple, field) => tuple.selectDynamic(field))
    end listPathGet
    Try(listPathGet(this, path.split("/").map(_.trim)).tuple).toEither.leftMap(
      t =>
        s"""Error '${t.getLocalizedMessage}' trying to get the value with the path $path"""
    )
  end getValue

  @SuppressWarnings(Array("scalafix:DisableSyntax.throw"))
  def get(path: String): Any =
    getValue(path) match
      case Left(error)  => throw Exception(error)
      case Right(value) => value
    end match
end DynamicTuple

extension (tuple: Tuple)
  @SuppressWarnings(
    Array(
      "scalafix:DisableSyntax.asInstanceOf",
      "scalafix:DisableSyntax.defaultArgs"
    )
  )
  def replace(
    path: String,
    value: Any,
    schema: Option[Schema] = None
  ): Either[String, Tuple] =
    val updatedTuple =
      DynamicTuple(tuple).replace(path, value).map(_.tuple.asInstanceOf[Tuple])
    schema.fold(updatedTuple)(schema =>
      updatedTuple.flatMap(t => parseTuple(t, schema))
    )
  end replace
end extension

given Conversion[String, Int] = Integer.parseInt(_)

given Conversion[Tuple, DynamicTuple] = DynamicTuple(_)

given Conversion[DynamicTuple, Tuple] =
  case v: Tuple => v
end given
