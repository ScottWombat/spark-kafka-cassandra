import Dependencies._

val sparkVersion = "3.5.0"
val cassandraVersion = "3.4.1"
val derbyVersion = "10.4.1.3"

ThisBuild / scalaVersion     := "2.12.18"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"
ThisBuild / assemblyMergeStrategy  := {
  case PathList("META-INF/services/org.apache.spark.sql.sources.DataSourceRegister") => MergeStrategy.concat
  case PathList("META-INF","services",xs @ _*) => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

lazy val root = (project in file("."))
  .settings(
    name := "Hello",
    libraryDependencies ++=  Seq(
      "org.apache.derby" % "derby" % derbyVersion,
      "org.apache.spark" % "spark-core_2.12" % sparkVersion,
      "org.apache.spark" % "spark-sql_2.12" % sparkVersion,
      "org.apache.spark" % "spark-streaming_2.12" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.4.1",
      "com.datastax.spark" %% "spark-cassandra-connector-driver" % "3.4.1",
      "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0",
      "org.apache.hadoop" % "hadoop-client" % "3.3.4",
      "io.dropwizard.metrics" % "metrics-core" % "4.2.22",
      "com.github.jnr" % "jnr-posix" % "3.1.18"
    )
  )

//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assembly / assemblyJarName := "hello-spark_1.0.jar"

resolvers in Global ++= Seq(
  "Sbt plugins"                   at "https://dl.bintray.com/sbt/sbt-plugin-releases",
  "Maven Central Server"          at "https://repo1.maven.org/maven2",
  "TypeSafe Repository Releases"  at "https://repo.typesafe.com/typesafe/releases/",
  "TypeSafe Repository Snapshots" at "https://repo.typesafe.com/typesafe/snapshots/"
)

