package it.agilelab.witboost.ontology.manager.domain.ontology.model

import scala.collection.Iterator
import scala.compiletime.summonAll
import scala.deriving.Mirror
import scala.language.{existentials, implicitConversions, postfixOps}

inline def stringEnumDecoder[T](using m: Mirror.SumOf[T]): String => T =
  val elemInstances =
    summonAll[Tuple.Map[m.MirroredElemTypes, ValueOf]].productIterator
      .asInstanceOf[Iterator[ValueOf[T]]]
      .map(_.value)
  val elemNames =
    summonAll[Tuple.Map[m.MirroredElemLabels, ValueOf]].productIterator
      .asInstanceOf[Iterator[ValueOf[String]]]
      .map(_.value)
  val mapping = (elemNames zip elemInstances).toMap
  name => mapping(name)
end stringEnumDecoder

inline def stringEnumEncoder[T](using m: Mirror.SumOf[T]): T => String =
  val elemInstances =
    summonAll[Tuple.Map[m.MirroredElemTypes, ValueOf]].productIterator
      .asInstanceOf[Iterator[ValueOf[T]]]
      .map(_.value)
  val elemNames =
    summonAll[Tuple.Map[m.MirroredElemLabels, ValueOf]].productIterator
      .asInstanceOf[Iterator[ValueOf[String]]]
      .map(_.value)
  val mapping = (elemInstances zip elemNames).toMap
  mapping.apply
end stringEnumEncoder
