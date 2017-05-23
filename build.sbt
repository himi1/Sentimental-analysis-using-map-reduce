lazy val commonSettings = Seq(
	version := "1.0",
	scalaVersion := "2.11.8",
	libraryDependencies ++= Seq(
		"org.apache.spark" %% "spark-core" % "2.0.1",
		"org.apache.spark" %% "spark-mllib" % "2.0.1"
	)
)

lazy val main = (project in file(".")).
	dependsOn(data).
	dependsOn(model).
    aggregate(data, model).
	settings(commonSettings:_*).
	settings(
		name := "RedWingedBlackbird",
		mainClass in Compile := Some("ebird.RedWingedBlackbird")
	)

lazy val data = (project in file("data")).settings(commonSettings:_*)

lazy val model = (project in file("model")).
	dependsOn(data).
	settings(commonSettings:_*)

