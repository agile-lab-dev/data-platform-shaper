package it.agilelab.dataplatformshaper.domain.model.schema

import cats.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*

enum DataType {
  def mode: Mode

  def changeMode(mode: Mode): DataType =
    if (this.mode.isValidConversion(mode))
      this match {
        case IntType(_)             => IntType(mode)
        case LongType(_)            => LongType(mode)
        case FloatType(_)           => FloatType(mode)
        case DoubleType(_)          => DoubleType(mode)
        case SqlDecimal(_)          => SqlDecimal(mode)
        case BooleanType(_)         => BooleanType(mode)
        case StringType(_)          => StringType(mode)
        case TimestampDataType(_)   => TimestampDataType(mode)
        case DateType(_)            => DateType(mode)
        case JsonType(_)            => JsonType(mode)
        case StructType(records, _) => StructType(records, mode)
      }
    else this

  case IntType(mode: Mode = Required)
  case LongType(mode: Mode = Required)
  case FloatType(mode: Mode = Required)
  case DoubleType(mode: Mode = Required)
  case SqlDecimal(mode: Mode = Required)
  case BooleanType(mode: Mode = Required)
  case StringType(mode: Mode = Required)
  case TimestampDataType(mode: Mode = Required)
  case DateType(mode: Mode = Required)
  case JsonType(mode: Mode = Required)
  case StructType(records: List[(String, DataType)], mode: Mode = Required)
}

enum Mode {

  def isValidConversion(newMode: Mode): Boolean = (this, newMode) match {
    case (Repeated, _)        => false
    case (Nullable, Required) => false
    case (_, _)               => true
  }

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
  TimestampDataType,
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
