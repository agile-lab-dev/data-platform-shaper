package it.agilelab.dataplatformshaper.domain.model.schema

import cats.*
import it.agilelab.dataplatformshaper.domain.model.schema.Mode.*

enum DataType {
  def mode: Mode

  def changeMode(mode: Mode): DataType =
    if (this.mode.isValidConversion(mode))
      this match {
        case SqlInt(_)             => SqlInt(mode)
        case SqlLong(_)            => SqlLong(mode)
        case SqlFloat(_)           => SqlFloat(mode)
        case SqlDouble(_)          => SqlDouble(mode)
        case SqlDecimal(_)         => SqlDecimal(mode)
        case SqlBool(_)            => SqlBool(mode)
        case SqlString(_)          => SqlString(mode)
        case SqlTimestamp(_)       => SqlTimestamp(mode)
        case SqlDate(_)            => SqlDate(mode)
        case SqlJson(_)            => SqlJson(mode)
        case SqlStruct(records, _) => SqlStruct(records, mode)
      }
    else this

  case SqlInt(mode: Mode = Required)
  case SqlLong(mode: Mode = Required)
  case SqlFloat(mode: Mode = Required)
  case SqlDouble(mode: Mode = Required)
  case SqlDecimal(mode: Mode = Required)
  case SqlBool(mode: Mode = Required)
  case SqlString(mode: Mode = Required)
  case SqlTimestamp(mode: Mode = Required)
  case SqlDate(mode: Mode = Required)
  case SqlJson(mode: Mode = Required)
  case SqlStruct(records: List[(String, DataType)], mode: Mode = Required)
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

export DataType.SqlString as StringType
export DataType.SqlInt as IntType
export DataType.SqlDate as DateType
export DataType.SqlTimestamp as TimestampDataType
export DataType.SqlStruct as StructType
export DataType.SqlDouble as DoubleType
export DataType.SqlFloat as FloatType
export DataType.SqlLong as LongType
export DataType.SqlBool as BooleanType
export DataType.SqlJson as JsonType

type Schema = StructType

given Eq[Mode] with
  def eqv(x: Mode, y: Mode): Boolean = (x, y) match
    case (Required, Required) => true
    case (Repeated, Repeated) => true
    case (Nullable, Nullable) => true
    case _                    => false
  end eqv
end given
