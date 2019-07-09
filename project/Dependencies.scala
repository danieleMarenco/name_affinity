import sbt._

object Dependencies {
  lazy val logbackClassicVersion = "1.2.3"
  lazy val scalaLoggingVersion   = "3.9.0"
  lazy val scalaTestVersion      = "3.0.5"
//  lazy val slf4jVersion          = "1.7.25"
  lazy val typesafeVersion       = "1.3.3"

  object Logging {
    lazy val prodDeps = Seq(
      "com.typesafe.scala-logging" %% "scala-logging"   % scalaLoggingVersion,
      "ch.qos.logback"             % "logback-classic"  % logbackClassicVersion // required by scala-logging
//      "org.slf4j"                  % "log4j-over-slf4j" % slf4jVersion // mandatory when log4j gets excluded
    )

    lazy val excludeDeps: Seq[ExclusionRule] = Seq(
      ExclusionRule("org.slf4j", "slf4j-log4j12")
//      ExclusionRule("log4j", "log4j")
    )
  }

  lazy val prodDeps: Seq[ModuleID] = Seq(
    "com.typesafe" % "config" % typesafeVersion
  ) ++ Logging.prodDeps

  lazy val testDeps: Seq[ModuleID] = Seq(
    "org.scalatest" %% "scalatest" % scalaTestVersion
  ).map(_ % Test)

  lazy val excludeDeps: Seq[ExclusionRule] = Nil ++ Logging.excludeDeps

}
