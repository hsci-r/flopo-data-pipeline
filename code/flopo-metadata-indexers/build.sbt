name := "flopo-metadata-indexers"

version := "0.1"

scalaVersion := "2.13.6"

javacOptions ++= Seq("--release", "11")

scalacOptions ++= Seq("-release", "11")

libraryDependencies ++= Seq(
  "org.rogach" %% "scallop" % "4.0.3",
  "com.univocity" % "univocity-parsers" % "2.9.1",

  "org.scala-lang.modules" %% "scala-xml" % "2.0.0",
  "org.json4s" %% "json4s-native" % "4.0.0",
  "fi.hsci" %% "octavo-indexer" % "1.2.4",
  "org.apache.lucene" % "lucene-core" % "8.9.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "8.9.0",

  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.31",
  "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.0",
  "junit" % "junit" % "4.13.2" % "test"
)

resolvers ++= Seq(
  Resolver.mavenLocal
)

import sbtassembly.AssemblyPlugin.defaultUniversalScript

ThisBuild / assemblyPrependShellScript := Some(defaultUniversalScript(shebang = true))

assembly / assemblyOutputPath := file("flopo-metadata-indexers")

ThisBuild / assemblyMergeStrategy := {
  case PathList("org", "apache", "lucene", "codecs", "blocktreeords", "BlockTreeOrdsPostingsFormat.class") => MergeStrategy.first // override badly named contrib codec
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
