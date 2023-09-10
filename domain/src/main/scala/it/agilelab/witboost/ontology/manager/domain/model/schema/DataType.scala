package it.agilelab.witboost.ontology.manager.domain.model.schema

import cats.*
import org.datatools.bigdatatypes.basictypes.SqlType.*
import org.datatools.bigdatatypes.basictypes.SqlTypeMode.*

export org.datatools.bigdatatypes.basictypes.SqlType as DataType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlString as StringType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlInt as IntType
export org.datatools.bigdatatypes.basictypes.SqlType.SqlStruct as StructType
export org.datatools.bigdatatypes.basictypes.SqlTypeMode as Mode

type Schema = StructType

given Eq[Mode] with
  def eqv(x: Mode, y: Mode): Boolean = (x, y) match
    case (Required, Required) => true
    case (Repeated, Repeated) => true
    case (Nullable, Nullable) => true
    case _                    => false
  end eqv
end given
