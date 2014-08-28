name := "sparkstreaming-algebird-algorithms-demo"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.0.2",
  "com.twitter" %% "algebird-core" % "0.7.0",
  "org.apache.spark" %% "spark-streaming" % "1.0.2",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

