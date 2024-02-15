package it.agilelab.dataplatformshaper.domain.model.schema

import cats.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*

enum DataType {
  def mode: Mode

  case IntType(mode: Mode = Required, constraints: Option[String] = None)
  case LongType(mode: Mode = Required, constraints: Option[String] = None)
  case FloatType(mode: Mode = Required, constraints: Option[String] = None)
  case DoubleType(mode: Mode = Required, constraints: Option[String] = None)
  case SqlDecimal(mode: Mode = Required, constraints: Option[String] = None)
  case BooleanType(mode: Mode = Required, constraints: Option[String] = None)
  case StringType(mode: Mode = Required, constraints: Option[String] = None)
  case TimestampType(
      mode: Mode = Required,
      constraints: Option[String] = None
  )
  case DateType(mode: Mode = Required, constraints: Option[String] = None)
  case JsonType(mode: Mode = Required)
  case StructType(records: List[(String, DataType)], mode: Mode = Required)
}

enum Mode {

  case Nullable

  case Repeated

  case Required
}

export DataType.{
  IntType,
  LongType,
  FloatType,
  DoubleType,
  SqlDecimal,
  BooleanType,
  StringType,
  TimestampType,
  DateType,
  JsonType,
  StructType
}

type Schema = StructType

given Eq[Mode] with
  def eqv(x: Mode, y: Mode): Boolean = (x, y) match
    case (Required, Required) => true
    case (Repeated, Repeated) => true
    case (Nullable, Nullable) => true
    case _                    => false
  end eqv
end given
