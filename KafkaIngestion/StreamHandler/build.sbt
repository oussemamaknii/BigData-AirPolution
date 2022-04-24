name := "Stream Handler"

version := "1.0"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.2.1" % "provided",
     "org.apache.spark" %% "spark-sql" % "3.2.1" % "provided"
)