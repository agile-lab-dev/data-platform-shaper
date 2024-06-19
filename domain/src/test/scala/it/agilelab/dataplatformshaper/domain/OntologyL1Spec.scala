package it.agilelab.dataplatformshaper.domain

import cats.data.EitherT
import cats.effect.IO
import io.chrisdavenport.mules.caffeine.CaffeineCache
import io.chrisdavenport.mules.{Cache, TimeSpec}
import it.agilelab.dataplatformshaper.domain.common.db.Repository
import it.agilelab.dataplatformshaper.domain.model.*
import it.agilelab.dataplatformshaper.domain.model.Relationship.hasPart
import it.agilelab.dataplatformshaper.domain.model.schema.*
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError
import it.agilelab.dataplatformshaper.domain.service.ManagementServiceError.*
import org.scalatest.Inside.inside

import scala.concurrent.duration.*
import scala.language.postfixOps
import scala.util.Right

class OntologyL1Spec extends CommonSpec:

  given cache: Cache[IO, String, EntityType] = CaffeineCache
    .build[IO, String, EntityType](
      Some(TimeSpec.unsafeFromDuration(1800.second)),
      None,
      None
    )
    .unsafeRunSync()

  "Checking the existence of a non existing Trait" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trms, _, _, _) = getManagementServices(repository)
        trms.exist("NonExistentTrait")
      } asserting (res => res should be(Right(false)))
    }
  }

  "Creating a trait" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trms, _, _, _) = getManagementServices(repository)
        (for {
          _ <- EitherT(trms.create(Trait("ANewTrait", None)))
          _ <- EitherT(trms.create(Trait("Help", Some("ANewTrait"))))
          res <- EitherT(trms.exist("ANewTrait"))
        } yield res).value
      } asserting (res => res should be(Right(true)))
    }
  }

  "Bulk creation of traits and their relationships" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trms, _, _, _) = getManagementServices(repository)

        val request = BulkTraitsCreationRequest(
          List(Trait("A", None), Trait("B", Some("A")), Trait("C", None)),
          List(("B", hasPart, "C"))
        )
        trms.create(request)
      } asserting (res =>
        res should matchPattern {
          case BulkTraitsCreationResponse(
                List((_, None), (_, None), (_, None)),
                List((_, None))
              ) =>
        }
      )
    }
  }

  "Bulk creation of traits and their relationships with a wrong request" - {
    "fails" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trms, _, _, _) = getManagementServices(repository)

        val request = BulkTraitsCreationRequest(
          List(Trait("D", None), Trait("E", Some("G")), Trait("F", None)),
          List(("E", hasPart, "F"))
        )
        trms.create(request)
      } asserting (res =>
        res should matchPattern {
          case BulkTraitsCreationResponse(
                List((_, None), (_, Some(_)), (_, None)),
                List((_, Some(_)))
              ) =>
        }
      )
    }
  }

  "Creating a trait with a missing father" - {
    "fails" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trms, _, _, _) = getManagementServices(repository)
        (for {
          _ <- EitherT(
            trms.create(Trait("YetAnotherTrait", Some("AMissingTrait")))
          )
        } yield ()).value
      } asserting (res => res.isLeft should be(true))
    }
  }

  "Listing all traits" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trms, _, _, _) = getManagementServices(repository)
        (for {
          _ <- EitherT(trms.create(Trait("ListTrait1", None)))
          _ <- EitherT(trms.create(Trait("ListTrait2", Some("ListTrait1"))))
          res <- EitherT(trms.list())
        } yield res).value
      } asserting {
        case Right(list) =>
          list should contain allElementsOf List("ListTrait1", "ListTrait2")
        case Left(error) =>
          fail(s"Expected a list of traits but got an error: $error")
      }
    }
  }

  "Deleting a trait" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trms, _, _, _) = getManagementServices(repository)
        (for {
          _ <- EitherT(trms.create(Trait("DeleteTrait", None)))
          _ <- EitherT(trms.delete("DeleteTrait"))
          res <- EitherT(trms.exist("DeleteTrait"))
        } yield res).value
      } asserting (res => res should be(Right(false)))
    }
  }

  "Deleting a trait with existing links" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trms, _, _, _) = getManagementServices(repository)
        (for {
          _ <- EitherT(trms.create(Trait("LinkTrait1", None)))
          _ <- EitherT(trms.create(Trait("LinkTrait2", None)))
          _ <- EitherT(trms.link("LinkTrait1", hasPart, "LinkTrait2"))
          _ <- EitherT(trms.delete("LinkTrait2"))
          res <- EitherT(trms.exist("LinkTrait2"))
        } yield res).value
      } asserting {
        case Left(error) => error shouldBe a[ManagementServiceError]
        case Right(_) =>
          fail("Expected a ManagementServiceError, but got success instead")
      }
    }
  }

  "Deleting a trait while an entity has it" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      val exampleSchema: StructType = StructType(
        List("ExampleField1" -> StringType(), "ExampleField2" -> StringType())
      )

      val exampleEntityType =
        EntityType("ExampleEntityType", Set("ExampleTrait"), exampleSchema)

      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trms, tservice, _, _) = getManagementServices(repository)
        (for {
          _ <- EitherT(trms.create(Trait("ExampleTrait", None)))
          _ <- EitherT(tservice.create(exampleEntityType))
          _ <- EitherT(trms.delete("ExampleTrait"))
          res <- EitherT(trms.exist("ExampleTrait"))
        } yield res).value
      } asserting {
        case Left(error) => error shouldBe a[ManagementServiceError]
        case Right(_) =>
          fail("Expected a ManagementServiceError, but got success instead")
      }
    }
  }

  "Deleting a trait while another trait is its subClass" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )

      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trms, _, _, _) = getManagementServices(repository)
        (for {
          _ <- EitherT(trms.create(Trait("FatherExampleTrait", None)))
          _ <- EitherT(
            trms.create(Trait("SonExampleTrait", Some("FatherExampleTrait")))
          )
          res <- EitherT(trms.delete("FatherExampleTrait"))
        } yield res).value
      } asserting {
        case Left(error) => error shouldBe a[ManagementServiceError]
        case Right(_) =>
          fail("Expected a ManagementServiceError, but got success instead")
      }
    }
  }

  "Linking a trait to another trait" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trms, _, _, _) = getManagementServices(repository)
        (for {
          _ <- EitherT(trms.create(Trait("DataProductComponent", None)))
          _ <- EitherT(
            trms.create(Trait("OutputPort", Some("DataProductComponent")))
          )
          _ <- EitherT(trms.create(Trait("DataProduct", None)))
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
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, tms, ims, _) = getManagementServices(repository)
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
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, tms, ims, _) = getManagementServices(repository)
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
            error should matchPattern { case ManagementServiceError(_) =>
            }
        }
      )
    }
  }

  "Unlinking two traits that have associated linked instances should trigger an error" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, _, _, _) = getManagementServices(repository)
        trservice.unlink("DataProduct", Relationship.hasPart, "OutputPort")
      } asserting (res =>
        res should matchPattern {
          case Left(ManagementServiceError(List(error)))
              if error.contains("have linked instances") =>
        }
      )
    }
  }

  "Link with relationship hasPart two instances and one of them is not related to the proper trait" - {
    "fails" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trservice, tms, ims, _) = getManagementServices(repository)
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
        res should matchPattern {
          case Left(ManagementServiceError(List(error)))
              if error.contains("invalid relationship") =>
        }
      )
    }
  }

  "Link with relationship hasPart when traits are inherited" - {
    "works" in {
      val session = getSession[IO](
        graphdbType,
        "localhost",
        "dba",
        "mysecret",
        "repo1",
        false
      )
      session.use { session =>
        val repository: Repository[IO] = getRepository[IO](session)
        val (trs, tms, ims, _) = getManagementServices(repository)
        (for {
          _ <- EitherT(trs.create(Trait("Trait1", None)))
          _ <- EitherT(trs.create(Trait("Trait2", None)))
          _ <- EitherT(trs.link("Trait1", Relationship.hasPart, "Trait2"))
          _ <- EitherT(
            tms.create(
              EntityType(
                "CommonType1",
                Set("Trait1"),
                StructType(List("name" -> StringType())): Schema
              )
            )
          )
          _ <- EitherT(
            tms.create(
              EntityType(
                "CommonType2",
                Set("Trait2"),
                StructType(List("name" -> StringType())): Schema
              )
            )
          )
          commonType1 <- EitherT(tms.read("CommonType1"))
          commonType2 <- EitherT(tms.read("CommonType2"))
          _ <- EitherT(
            tms.create(
              EntityType(
                "Type1",
                StructType(List("name" -> StringType())): Schema,
                commonType1
              )
            )
          )
          _ <- EitherT(
            tms.create(
              EntityType(
                "Type2",
                StructType(List("name" -> StringType())): Schema,
                commonType2
              )
            )
          )
          inst1 <- EitherT(ims.create("Type1", Tuple1("name" -> "tp1")))
          inst2 <- EitherT(ims.create("Type2", Tuple1("name" -> "tp22")))
          res <- EitherT(ims.link(inst1, Relationship.hasPart, inst2))
        } yield res).value
      } asserting (res =>
        res should matchPattern { case Right(_) =>
        }
      )
    }
  }
end OntologyL1Spec
