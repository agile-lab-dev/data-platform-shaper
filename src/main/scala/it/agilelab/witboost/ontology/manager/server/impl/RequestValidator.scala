package it.agilelab.witboost.ontology.manager.server.impl

import cats.data.Kleisli
import cats.effect.unsafe.implicits.global
import cats.effect.{Async, IO}
import cats.implicits.*
import com.atlassian.oai.validator.OpenApiInteractionValidator
import com.atlassian.oai.validator.model.Request.Method
import com.atlassian.oai.validator.model.SimpleRequest
import it.agilelab.witboost.ontology.manager.definitions.ValidationError
import org.http4s.Method.*
import org.http4s.circe.jsonEncoderOf
import org.http4s.headers.`Content-Type`
import org.http4s.{HttpRoutes, MediaType, Request, Response, Status}

import scala.jdk.CollectionConverters.*

@SuppressWarnings(
  Array(
    "org.wartremover.warts.AsInstanceOf",
    "org.wartremover.warts.OptionPartial",
    "org.wartremover.warts.ToString",
    "org.wartremover.warts.Var"
  )
)
object RequestValidator:

  def apply[F[_]: Async](
      service: HttpRoutes[F],
      interfaceString: String
  ): HttpRoutes[F] =
    val validator: OpenApiInteractionValidator = OpenApiInteractionValidator
      .createForInlineApiSpecification(interfaceString)
      .build()
    Kleisli { (req: Request[F]) =>
      val headers = req.headers.headers.map(p => (p.name.toString, p.value))
      val method = req.method match
        case GET     => Method.GET
        case POST    => Method.POST
        case PUT     => Method.PUT
        case PATCH   => Method.PATCH
        case DELETE  => Method.DELETE
        case HEAD    => Method.HEAD
        case OPTIONS => Method.OPTIONS
        case TRACE   => Method.TRACE
      var simpleRequestBuilder =
        SimpleRequest.Builder(method, req.uri.renderString)
      headers.foreach { header =>
        simpleRequestBuilder =
          simpleRequestBuilder.withHeader(header._1, header._2)
      }
      val body = new String(
        req.body.compile.toList
          .asInstanceOf[IO[List[Byte]]]
          .unsafeRunSync()
          .toArray,
        "UTF8"
      )

      simpleRequestBuilder.withBody(body)
      val validationReport =
        validator.validateRequest(simpleRequestBuilder.build())
      if (validationReport.hasErrors)
        val validationError = ValidationError(
          validationReport.getMessages.asScala.map(_.toString).toVector
        )
        Response(Status.BadRequest)
          .withContentType(
            `Content-Type`(MediaType.parse("application/json").toOption.get)
          )
          .withEntity(validationError)(jsonEncoderOf[F, ValidationError])
          .pure
      else service(req)
    }
  end apply
end RequestValidator
