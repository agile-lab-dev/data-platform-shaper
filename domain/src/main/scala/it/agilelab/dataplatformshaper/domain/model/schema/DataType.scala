package it.agilelab.dataplatformshaper.domain.model.schema

import cats.*
import org.datatools.bigdatatypes.basictypes.SqlType.*
import org.datatools.bigdatatypes.basictypes.SqlTypeMode
import org.datatools.bigdatatypes.basictypes.SqlTypeMode.*

export org.datatools.bigdatatypes.basictypes.SqlType as DataType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlString as StringType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlInt as IntType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlDate as DateType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlTimestamp as TimestampDataType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlStruct as StructType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlDouble as DoubleType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlFloat as FloatType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlLong as LongType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlBool as BoolType
export org.datatools.bigdatatypes.basictypes.SqlTypeMode as Mode

type Schema = StructType

given Eq[SqlTypeMode] with
  def eqv(x: SqlTypeMode, y: SqlTypeMode): Boolean = (x, y) match
    case (Required, Required) => true
    case (Repeated, Repeated) => true
    case (Nullable, Nullable) => true
    case _                    => false
  end eqv
end given
