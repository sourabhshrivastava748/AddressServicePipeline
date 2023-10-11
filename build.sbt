ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

libraryDependencies += "com.github.sh0nk" % "matplotlib4j" % "0.5.0"
libraryDependencies += "com.googlecode.libphonenumber" % "libphonenumber" % "8.13.0"
libraryDependencies += "com.google.guava" % "guava" % "14.0.1"
libraryDependencies += "me.xdrop" % "fuzzywuzzy" % "1.4.0"
libraryDependencies += "com.sun.mail" % "javax.mail" % "1.6.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "3.3.4"
libraryDependencies += "com.mysql" % "mysql-connector-j" % "8.0.31"
libraryDependencies += "org.mongodb" % "mongo-java-driver" % "3.12.12"
libraryDependencies += "commons-codec" % "commons-codec" % "1.15"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.0"
libraryDependencies += "com.bettercloud" % "vault-java-driver" % "4.0.0"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.13.0"
libraryDependencies += "org.springframework" % "spring-web" % "5.3.13"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1" % "provided"

lazy val root = (project in file("."))
  .settings(
    name := "AddressServicePipeline"
  )
