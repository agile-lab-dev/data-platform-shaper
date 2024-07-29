ThisBuild / libraryDependencySchemes ++= Seq(
  "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always
)

addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.12.1")

addSbtPlugin("org.typelevel" % "sbt-tpolecat" % "0.5.1")

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.12.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.1.0")

addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.6.4")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.2.0")

addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.10.0")

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.5.2")

addSbtPlugin("com.lightbend.sbt" % "sbt-javaagent" % "0.1.6")

addSbtPlugin("com.github.sbt" % "sbt-site-paradox" % "1.7.0" exclude("org.scala-lang.modules", "scala-java8-compat_2.12"))

addDependencyTreePlugin
