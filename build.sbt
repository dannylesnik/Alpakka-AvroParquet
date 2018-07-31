
name := "Alpakka-AvroParquet"

version := "0.1"

scalaVersion := "2.12.6"

//Revolver.settings
enablePlugins(JavaAppPackaging)

dockerEntrypoint := Seq("bin/%s" format executableScriptName.value)
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.14"
libraryDependencies +=     "org.apache.parquet" % "parquet-avro" % "1.8.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0"
libraryDependencies += "org.specs2" %% "specs2-core" % "4.3.2" % Test
libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.14" % Test
libraryDependencies += "junit" % "junit" % "4.12" % Test