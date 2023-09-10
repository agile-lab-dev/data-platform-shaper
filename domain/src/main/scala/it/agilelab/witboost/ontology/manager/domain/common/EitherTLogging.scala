package it.agilelab.witboost.ontology.manager.domain.common

import cats.Functor
import cats.data.EitherT
import cats.effect.Sync
import org.typelevel.log4cats.Logger

object EitherTLogging:
  inline def errorT[F[_]: Sync: Logger, A](msg: String): EitherT[F, A, Unit] =
    EitherT[F, A, Unit](
      summon[Functor[F]].map(Logger[F].error(msg))(Right[A, Unit])
    )
  inline def infoT[F[_]: Sync: Logger, A](msg: String): EitherT[F, A, Unit] =
    EitherT[F, A, Unit](
      summon[Functor[F]].map(Logger[F].info(msg))(Right[A, Unit])
    )
  inline def debugT[F[_]: Sync: Logger, A](msg: String): EitherT[F, A, Unit] =
    EitherT[F, A, Unit](
      summon[Functor[F]].map(Logger[F].debug(msg))(Right[A, Unit])
    )
  inline def traceT[F[_]: Sync: Logger, A](msg: String): EitherT[F, A, Unit] =
    EitherT[F, A, Unit](
      summon[Functor[F]].map(Logger[F].trace(msg))(Right[A, Unit])
    )
end EitherTLogging
