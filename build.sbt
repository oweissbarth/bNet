name := "bayesian"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-jdk14")) }


/*JUnit*/
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test"

/*XML */
libraryDependencies += "org.scala-lang.modules" % "scala-xml_2.11" % "1.0.2"

/*Spark*/
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"


/*Logging*/
//libraryDependencies += "ch.qos.logback" %  "logback-classic" % "1.1.7"
//libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0"

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)

    