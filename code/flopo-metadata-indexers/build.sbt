name := "flopo-metadata-indexers"

version := "0.1"

scalaVersion := "2.13.5"

idePackagePrefix := Some("fi.hsci")

libraryDependencies ++= Seq(
  "org.rogach" %% "scallop" % "4.0.2",
  "com.univocity" % "univocity-parsers" % "2.9.1",

  "org.json4s" %% "json4s-native" % "3.7.0-M15",
  "fi.hsci" %% "octavo-indexer" % "1.1.8",
  "org.apache.lucene" % "lucene-core" % "8.8.1",
  "org.apache.lucene" % "lucene-analyzers-common" % "8.8.1",

  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.30",
  "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.0-RC1",
  "junit" % "junit" % "4.13.2" % "test"
)

resolvers ++= Seq(
  Resolver.mavenLocal
)

//import sbtassembly.AssemblyPlugin.defaultUniversalScript

//assemblyOption in assembly := (assemblyOption in assembly).value.copy(prependShellScript = Some(defaultUniversalScript(shebang = true)))

assemblyOutputPath in assembly := file("flopo-metadata-indexers")

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "lucene", "codecs", "blocktreeords", "BlockTreeOrdsPostingsFormat.class") => MergeStrategy.first // override badly named contrib codec
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
