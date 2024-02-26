package it.agilelab.dataplatformshaper.domain.model.l1

import it.agilelab.dataplatformshaper.domain.common.{
  stringEnumDecoder,
  stringEnumEncoder
}

enum Relationship:
  case hasPart
  case mappedTo
  case mappedFrom
  def getNamespace: String =
    this match
      case Relationship.hasPart =>
        "http://www.w3.org/2001/sw/BestPractices/OEP/SimplePartWhole/part.owl#"
      case Relationship.mappedTo =>
        "https://w3id.org/agile-dm/ontology/"
      case Relationship.mappedFrom =>
        "https://w3id.org/agile-dm/ontology/"
    end match
  end getNamespace
end Relationship

given Conversion[String, Relationship] with
  def apply(str: String): Relationship =
    stringEnumDecoder[Relationship].apply(str)
end given

given Conversion[Relationship, String] with
  def apply(attr: Relationship): String =
    stringEnumEncoder[Relationship].apply(attr)
end given

export Relationship.hasPart as hasPart
export Relationship.mappedTo as mappedTo
export Relationship.mappedFrom as mappedFrom
