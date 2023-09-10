package it.agilelab.witboost.ontology.manager.domain.model.l1

import it.agilelab.witboost.ontology.manager.domain.common.{
  stringEnumDecoder,
  stringEnumEncoder
}

enum Relationship:
  def getPrefix: String =
    this match
      case hasPart => "part"
    end match
  end getPrefix
  def getNamespace: String =
    this match
      case hasPart =>
        "http://www.w3.org/2001/sw/BestPractices/OEP/SimplePartWhole/part.owl#"
    end match
  end getNamespace

  case hasPart
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
