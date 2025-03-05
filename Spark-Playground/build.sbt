// build.sbt
name := "Spark-Playground"

version := "0.1"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.4",
  "org.apache.spark" %% "spark-sql" % "3.5.4",
  "org.apache.spark" %% "spark-streaming" % "3.5.4",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.4",
  "com.softwaremill.sttp.client4" %% "core" % "4.0.0-M6",
  "org.apache.kafka" % "kafka-clients" % "3.0.0",
//  "org.scala-sbt" % "compiler-interface" % "1.3.5",
  "org.apache.logging.log4j" % "log4j-core" % "2.14.1",
  "org.apache.logging.log4j" % "log4j-api" % "2.14.1"
)

//resolvers += Resolver.sonatypeRepo("public")