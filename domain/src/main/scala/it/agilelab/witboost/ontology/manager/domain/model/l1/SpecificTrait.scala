package it.agilelab.witboost.ontology.manager.domain.model.l1

import it.agilelab.witboost.ontology.manager.domain.common.{
  stringEnumDecoder,
  stringEnumEncoder
}
import it.agilelab.witboost.ontology.manager.domain.model.l0.Trait
import it.agilelab.witboost.ontology.manager.domain.model.schema.*
import it.agilelab.witboost.ontology.manager.domain.model.schema.Schema

@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
enum GenericTrait(
    val schema: Schema = StructType(List.empty[(String, DataType)])
) extends Trait:
  case Versionable
      extends GenericTrait(StructType(List("version" -> StringType())))
end GenericTrait

enum DataPlatformTrait extends Trait:
  case DataCollection
  case DataProduct
end DataPlatformTrait

enum DataProductComponent extends Trait:
  case OutputPort
  case InputPort
  case Workload
end DataProductComponent

type SpecificTrait = GenericTrait | DataPlatformTrait | DataProductComponent

given Conversion[String, SpecificTrait] with
  def apply(str: String): SpecificTrait =
    str match
      case "Versionable" => stringEnumDecoder[GenericTrait].apply(str)
      case "DataCollection" | "DataProduct" =>
        stringEnumDecoder[DataPlatformTrait].apply(str)
      case "OutputPort" | "InputPort" | "Workload" =>
        stringEnumDecoder[DataProductComponent].apply(str)
  end apply
end given

given Conversion[SpecificTrait, String] with
  def apply(attr: SpecificTrait): String =
    attr match
      case attr: GenericTrait =>
        stringEnumEncoder[GenericTrait].apply(attr)
      case attr: DataPlatformTrait =>
        stringEnumEncoder[DataPlatformTrait].apply(attr)
      case attr: DataProductComponent =>
        stringEnumEncoder[DataProductComponent].apply(attr)
  end apply
end given

export GenericTrait.Versionable as Versionable
export DataPlatformTrait.DataCollection as DataCollection
export DataPlatformTrait.DataProduct as DataProduct
export DataProductComponent.OutputPort as OutputPort
export DataProductComponent.InputPort as InputPort
export DataProductComponent.Workload as Workload
