name := "spark-citus"

version := "0.2"

scalaVersion := "2.11.8"

val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % sparkVersion % "provided").
    exclude("org.apache.spark", "spark-network-common_2.11").
    exclude("org.apache.spark", "spark-network-shuffle_2.11"),
  // avoid an ivy bug
  "org.apache.spark" %% "spark-network-common" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-network-shuffle" % sparkVersion % "provided",
  ("org.scalikejdbc" %% "scalikejdbc" % "3.3.0").
    exclude("org.slf4j", "slf4j-api"),
  ("org.postgresql" % "postgresql" % "9.3-1101-jdbc4").
    exclude("org.slf4j", "slf4j-api"),
  "org.scalatest" %% "scalatest" % "2.2.2" % "test"
)
