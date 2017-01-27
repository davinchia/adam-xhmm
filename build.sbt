name := "XHMM"

version := "1.0"

scalaVersion := "2.10.6"

resolvers += "Sonatype OSS Snapshots" at
  "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
  "org.apache.spark"   % "spark-core_2.10"      % "1.1.0",
  "org.apache.spark"   % "spark-mllib_2.10"     % "1.1.0",
  "org.apache.commons" % "commons-math3"        % "3.6.1",
  "com.storm-enroute"  %% "scalameter-core"     % "0.7"
)