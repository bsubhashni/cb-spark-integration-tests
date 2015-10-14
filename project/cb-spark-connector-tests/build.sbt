name := "cb-spark-connector-tests"

version := "1.0"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

autoScalaLibrary := false

fork in run := true

connectInput in run := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0",
  "org.apache.spark" %% "spark-streaming" % "1.5.0",
  "org.apache.spark" %% "spark-sql" % "1.5.0",
  "com.couchbase.client" %% "spark-connector" % "1.0.0-beta"
)

libraryDependencies +=   "com.couchbase.client" % "java-client" % "2.1.3"
libraryDependencies +=   "com.google.code.gson" % "gson" % "2.3.1"
libraryDependencies +=  "com.jcraft" % "jsch" % "0.1.53"



resolvers += Resolver.mavenLocal
resolvers += "Couchbase Repository" at "http://files.couchbase.com/maven2/"