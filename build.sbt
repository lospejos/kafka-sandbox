name := "kafka-sandbox"

scalaVersion := "2.11.7"

resolvers += "confluent" at "http://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  //  "org.apache.kafka" % "kafka-clients" % "0.10.0.0",
  "org.apache.kafka" %% "kafka" % "0.10.0.0-cp1",
  "io.confluent" % "kafka-avro-serializer" % "3.0.0"
)
