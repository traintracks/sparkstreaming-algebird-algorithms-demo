name := "sparkstreaming-algebird-algorithms-demo"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  // spark
  "org.apache.spark" %% "spark-core" % "1.0.2",
  "org.apache.spark" %% "spark-streaming" % "1.0.2",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.0.2",
// algebird
  "com.twitter" %% "algebird-core" % "0.7.0",
  // scalatest
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  // kafka
  "org.apache.kafka" %% "kafka" % "0.8.1.1"
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
