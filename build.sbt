name := "XHMM"

version := "1.0"

scalaVersion := "2.10.6"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.1"

libraryDependencies ++= Seq(
  "org.apache.spark"  % "spark-core_2.10"              % "1.1.0",
  "org.apache.spark"  % "spark-mllib_2.10"             % "1.1.0",
  "org.apache.commons"  % "commons-math3"              % "3.6.1"
)