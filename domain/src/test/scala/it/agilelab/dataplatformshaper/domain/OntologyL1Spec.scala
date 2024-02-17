package it.agilelab.dataplatformshaper.domain

import cats.data.EitherT
import cats.effect.{IO, Ref}
import it.agilelab.dataplatformshaper.domain.knowledgegraph.interpreter.{
  Rdf4jKnowledgeGraph,
  Session
}
import it.agilelab.dataplatformshaper.domain.model.l0.*
import it.agilelab.dataplatformshaper.domain.model.l1.Relationship
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
import it.agilelab.dataplatformshaper.domain.service.interpreter.{
  InstanceManagementServiceInterpreter,
  TraitManagementServiceInterpreter,
  TypeManagementServiceInterpreter
}
import org.scalatest.Inside.inside

import scala.language.postfixOps
import scala.util.Right

class OntologyL1Spec extends CommonSpec:

  given cache: Ref[IO, Map[String, EntityType]] =
    Ref[IO].of(Map.empty[String, EntityType]).unsafeRunSync()

  "Loading the base ontology" - {
    "works" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        repository.loadBaseOntologies()
      } asserting (_ => true shouldBe true)
    }
  }

  "Checking the existence of a non existing Trait" - {
    "works" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trms = TraitManagementServiceInterpreter[IO](repository)
        trms.exist("NonExistentTrait")
      } asserting (res => res should be(Right(false)))
    }
  }

  "Creating a trait" - {
    "works" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trms = TraitManagementServiceInterpreter[IO](repository)
        (for {
          _ <- EitherT(trms.create("ANewTrait", None))
          res <- EitherT(trms.exist("ANewTrait"))
        } yield res).value
      } asserting (res => res should be(Right(true)))
    }
  }

  "Linking a trait to another trait" - {
    "works" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trms = TraitManagementServiceInterpreter[IO](repository)
        (for {
          _ <- EitherT(trms.create("DataProductComponent", None))
          _ <- EitherT(trms.create("OutputPort", Some("DataProductComponent")))
          _ <- EitherT(trms.create("DataProduct", None))
          _ <- EitherT(
            trms
              .link("DataProduct", Relationship.hasPart, "DataProductComponent")
          )
          res <- EitherT(trms.linked("DataProduct", Relationship.hasPart))
        } yield res).value
      } asserting (res => res should be(Right(List("DataProductComponent"))))
    }
  }

  "Link with relationship hasPart different instances of entity types" - {
    "works" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trservice = new TraitManagementServiceInterpreter[IO](repository)
        val tms = TypeManagementServiceInterpreter[IO](trservice)
        val ims = InstanceManagementServiceInterpreter[IO](tms)
        (for {
          _ <- EitherT(
            tms.create(
              EntityType(
                "DataProductType",
                Set("DataProduct"),
                StructType(List("name" -> StringType())): Schema
              )
            )
          )
          _ <- EitherT(
            tms.create(
              EntityType(
                "OutputPortType",
                Set("OutputPort"),
                StructType(List("name" -> StringType())): Schema
              )
            )
          )
          dp <- EitherT(ims.create("DataProductType", Tuple1("name" -> "dp1")))
          op1 <- EitherT(ims.create("OutputPortType", Tuple1("name" -> "op1")))
          op2 <- EitherT(ims.create("OutputPortType", Tuple1("name" -> "op2")))
          _ <- EitherT(ims.link(dp, Relationship.hasPart, op1))
          _ <- EitherT(ims.link(dp, Relationship.hasPart, op2))
          ids <- EitherT(ims.linked(dp, Relationship.hasPart))
        } yield (ids, op1, op2)).value
      } asserting (res =>
        inside(res) {
          case Right(entity) =>
            entity(0) shouldBe List(entity(1), entity(2))
          case Left(_) =>
            true shouldBe false
        }
      )
    }
  }

  "Deleting an instance with linked instances should trigger an error" - {
    "works" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trservice = new TraitManagementServiceInterpreter[IO](repository)
        val tms = TypeManagementServiceInterpreter[IO](trservice)
        val ims = InstanceManagementServiceInterpreter[IO](tms)
        (for {
          dp <- EitherT(ims.create("DataProductType", Tuple1("name" -> "dp1")))
          op1 <- EitherT(ims.create("OutputPortType", Tuple1("name" -> "op1")))
          op2 <- EitherT(ims.create("OutputPortType", Tuple1("name" -> "op2")))
          _ <- EitherT(ims.link(dp, Relationship.hasPart, op1))
          _ <- EitherT(ims.link(dp, Relationship.hasPart, op2))
          le <- EitherT(ims.delete(dp))
        } yield le).value
      } asserting (res =>
        inside(res) {
          case Right(_) =>
            false shouldBe true
          case Left(error) =>
            error should matchPattern {
              case InstanceHasLinkedInstancesError(_) =>
            }
        }
      )
    }
  }

  "Unlinking two traits that have associated linked instances should trigger an error" - {
    "works" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trservice = new TraitManagementServiceInterpreter[IO](repository)
        trservice.unlink("DataProduct", Relationship.hasPart, "OutputPort")
      } asserting (res =>
        res should matchPattern {
          case Left(
                TraitsHaveLinkedInstancesError("DataProduct", "OutputPort")
              ) =>
        }
      )
    }
  }

  "Link with relationship hasPart two instances and one of them is not related to the proper trait" - {
    "fails" in {
      val session = Session[IO](
        graphdbType,
        "localhost",
        7201,
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository = Rdf4jKnowledgeGraph[IO](session)
        val trservice = new TraitManagementServiceInterpreter[IO](repository)
        val tms = TypeManagementServiceInterpreter[IO](trservice)
        val ims = InstanceManagementServiceInterpreter[IO](tms)
        (for {
          _ <- EitherT(
            tms.create(
              EntityType(
                "AnotherType",
                StructType(List("name" -> StringType())): Schema
              )
            )
          )
          dp <- EitherT(ims.create("DataProductType", Tuple1("name" -> "dp2")))
          at1 <- EitherT(ims.create("AnotherType", Tuple1("name" -> "at1")))
          res <- EitherT(ims.link(dp, Relationship.hasPart, at1))
        } yield res).value
      } asserting (res =>
        res should matchPattern { case Left(InvalidLinkType(_, "hasPart", _)) =>
        }
      )
    }
  }

end OntologyL1Spec
