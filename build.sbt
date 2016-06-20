name := "kafka-sandbox"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.8.2.1",
  "org.apache.spark" %% "spark-streaming" % "1.5.2",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.5.2"
)
