import Dependencies._

lazy val mainClassName = ""
lazy val sparkVersion  = "2.2.0"

lazy val root = project
  .in(file("."))
  .settings(
    excludeDependencies ++= excludeDeps,
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http-experimental" % "2.4.2",
      "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
      "org.apache.spark" % "spark-core_2.11" % sparkVersion
    ) ++ prodDeps ++ testDeps,
    mainClass in (Compile, run) := Some(mainClassName),
    mainClass in (Compile, packageBin) := Some(mainClassName),
    name := "name_affinity",
    parallelExecution in test := false,
    scalacOptions ++= Seq("-deprecation", "-encoding", "utf8"),
    scalaVersion := "2.11.7"
  )

/**
  * sbt-scalafmt plugin
  */
scalafmtOnCompile := true

/**
  * sbt-assembly plugin
  */
assemblyJarName in assembly := s"${name.value}.jar"
assemblyMergeStrategy in assembly := {
  case PathList("LICENSE", "LICENSE.txt", "NOTICE", "README") => MergeStrategy.discard
  case PathList(ps @ _*) if ps.last endsWith ".class"         => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
//mainClass in assembly := Some("Main")
test in assembly := {}

/**
  * sbt-docker plugin
  */
enablePlugins(DockerPlugin)

buildOptions in docker := BuildOptions(cache = false)

dockerfile in docker := {
  // The assembly task generates a fat JAR file
  val artifact           = assembly.value
  val artifactTargetPath = s"/app/${artifact.name}"

  new Dockerfile {
    from("openjdk:8-jre-alpine")
    copy(artifact, artifactTargetPath)
    maintainer("Daniele Marenco")
    entryPoint("java", "-jar", artifactTargetPath)
  }
}
