name := "bNet"

version := "1.0"

organization:="de.oweissbarth"

scalaVersion := "2.10.6"

//libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-jdk14")) }


/*JUnit*/
//libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % "test"
libraryDependencies += "org.scalactic" %% "scalactic" % "2.2.6"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.6" % "test"


/*XML */
//libraryDependencies += "org.scala-lang.modules" %% "scala-xml" % "1.0.2" //NOTE this is included in scala 2.10. Remmeber to readd when going to spark 2.0

/*Spark*/
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.6.1"
libraryDependencies += "com.databricks" %% "spark-csv" % "1.4.0"



libraryDependencies += "com.github.fommil.netlib" % "all" % "1.1.2"


/*Logging*/
//libraryDependencies += "ch.qos.logback" %  "logback-classic" % "1.1.7"
//libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0"

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=true",
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)

parallelExecution in test := false


    