import sbt._
import Keys._
import sbtassembly.AssemblyKeys._

object CassandraUtilBuild extends Build {
  // configure prompt to show current project
  override lazy val settings = super.settings :+ {
    shellPrompt := { s => Project.extract(s).currentProject.id + " > "}
  }

  val libVersion = "0.24"
  val datastaxVersion = "3.0.0"
  val slf4jVersion = "1.7.+"
  val guavaVersion = "17.0"
  val playVersion = "2.3.+"
  val cassandraVersion = "2.1.14"

  val scalaVersionsToCompile = Seq("2.10.5", "2.11.6")

  val baseDependencies = Seq(
    "junit" % "junit" % "4.7" % Test,
    "org.specs2" %% "specs2" % "2.4" % Test
  ).map(_.exclude("com.sun.jdmk", "jmxtools"))
    .map(_.exclude("com.sun.jmx", "jmxri"))

  lazy val defaultSettings =
    Defaults.coreDefaultSettings ++
      net.virtualvoid.sbt.graph.Plugin.graphSettings ++
      Seq(
        version := Option(System.getenv("BUILD_NUMBER")).map(x => s"$libVersion.$x").getOrElse {
          s"$libVersion${Option(System.getenv("BRANCH_ID")).map(x => s"-$x").getOrElse("")}-SNAPSHOT"
        },
        organization := "com.protectwise",
        crossScalaVersions := scalaVersionsToCompile,
        scalacOptions := Seq(
          "-feature",
          "-language:_",
          "-deprecation",
          "-unchecked",
          "-Xcheckinit",
          "-Xlint",
          "-Xverify",
          "-Yclosure-elim"
        ),
        resolvers ++= Seq(
          "Sonatype OSS Releases" at "http://oss.sonatype.org/content/repositories/releases"
        ),
        conflictWarning := ConflictWarning.disable,
        testOptions in Test += Tests.Argument("junitxml", "console"),
        publishTo <<= (isSnapshot) { isSnapshot: Boolean =>
          if (isSnapshot)
            Some("snapshots" at "https://some-repo/content/repositories/snapshots")
          else
            Some("releases" at "https://some-repo/content/repositories/releases")
        },
        credentials += Credentials(
          "Sonatype Nexus Repository Manager",
          "some-repo",
          "build",
          System.getenv("BUILD_PASSWORD")
        ),
        parallelExecution in Test := false,
        fork in Test := true,
        unmanagedResourceDirectories in Test ++= (managedResourceDirectories in Compile).value
      )

  lazy val root = Project(id = "cassandra-util", base = file("."), settings = defaultSettings) aggregate(protectwiseUtil, ccmTestingHelper, cqlWrapper, deletingCompactionStrategy)

  lazy val cqlWrapper = Project(
    id = "cql-wrapper", base = file("cql-wrapper"), settings = defaultSettings
  ).settings(
    fork in run := true,
    fork in Test := true,

    libraryDependencies ++= baseDependencies ++ Seq(
      "com.datastax.cassandra" % "cassandra-driver-core" % datastaxVersion,
      "com.google.code.findbugs" % "jsr305" % "1.3.+",
      "com.typesafe.play" %% "play-iteratees" % playVersion
    ),

    javaOptions in(Test, run) ++= Seq(
      "-d64",
      "-server",
      "-XX:+AggressiveOpts",
      "-XX:+UseConcMarkSweepGC",
      "-XX:+UseParNewGC",
      "-XX:NewRatio=1",
      "-XX:GCTimeRatio=19",
      "-XX:+HeapDumpOnOutOfMemoryError",
      "-XX:MaxDirectMemorySize=512M"
    ),

    javaOptions in Test ++= Seq(
      "-Xms2G",
      "-Xmx4G"
    ),

    javaOptions in run ++= Seq(
      "-Xms4G",
      "-Xmx8G"
    )
  ).dependsOn(protectwiseUtil)

  lazy val deletingCompactionStrategy = Project(
    id = "deleting-compaction-strategy", base = file("deleting-compaction-strategy"), settings = defaultSettings
  ).settings(
      version := Option(System.getenv("BUILD_NUMBER")).map(x => s"$libVersion.$x").getOrElse {
        s"$libVersion${Option(System.getenv("BRANCH_ID")).map(x => s"-$x").getOrElse("")}-SNAPSHOT"
      },
      fork in run := true,
      fork in Test := true,
      test in assembly := {},
      sources in (Compile,doc) := Seq.empty,
      publishArtifact in (Compile, packageDoc) := false,
      // This is a java project, don't put the scala version details in there.
      artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
        s"${artifact.name}-${module.revision}.${artifact.extension}"
      },

      libraryDependencies := baseDependencies.map {
        case d if !d.configurations.isDefined => d % Provided
        case d => d
      } ++ Seq(
        "org.apache.cassandra" % "cassandra-all" % cassandraVersion % Provided
      ),

      javaOptions in(Test, run) ++= Seq(
        "-d64",
        "-server",
        "-XX:+AggressiveOpts",
        "-XX:+UseConcMarkSweepGC",
        "-XX:+UseParNewGC",
        "-XX:NewRatio=1",
        "-XX:GCTimeRatio=19",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:MaxDirectMemorySize=512M"
      ),

      javaOptions in Test ++= Seq(
        "-Xms2G",
        "-Xmx4G"
      ),

      javaOptions in run ++= Seq(
        "-Xms4G",
        "-Xmx8G"
      )

    ).dependsOn(ccmTestingHelper % Test)
     .dependsOn(cqlWrapper % Test)

  lazy val ccmTestingHelper = Project(
    id = "ccm-testing-helper", base = file("ccm-testing-helper"), settings = defaultSettings
  ).settings(
      fork in run := true,
      fork in Test := true,

      // This is a java project, don't put the scala version details in the artifact name.
      artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
        s"${artifact.name}-${module.revision}.${artifact.extension}"
      },

      libraryDependencies := baseDependencies ++ Seq(
        "commons-io" % "commons-io" % "2.4",
        "fr.janalyse" %% "janalyse-jmx" % "0.7.2"
      ),

      javaOptions in(Test, run) ++= Seq(
        "-d64",
        "-server",
        "-XX:+AggressiveOpts",
        "-XX:+UseConcMarkSweepGC",
        "-XX:+UseParNewGC",
        "-XX:NewRatio=1",
        "-XX:GCTimeRatio=19",
        "-XX:+HeapDumpOnOutOfMemoryError",
        "-XX:MaxDirectMemorySize=512M"
      ),

      javaOptions in Test ++= Seq(
        "-Xms2G",
        "-Xmx4G"
      ),

      javaOptions in run ++= Seq(
        "-Xms4G",
        "-Xmx8G"
      )

    ).dependsOn(protectwiseUtil)

  lazy val protectwiseUtil = Project(
    id = "protectwise-util", base = file("protectwise-util"), settings = defaultSettings
  ).settings(
    libraryDependencies ++= baseDependencies ++ Seq(
      "com.typesafe" % "config" % "1.2.1",
      "com.google.guava" % "guava" % guavaVersion,

      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "ch.qos.logback" % "logback-classic" % "1.1.+",
      "org.slf4j" % "jul-to-slf4j" % slf4jVersion,
      "org.slf4j" % "log4j-over-slf4j" % slf4jVersion
      ).map(_.exclude("com.sun.jdmk", "jmxtools"))
      .map(_.exclude("com.sun.jmx", "jmxri"))
      .map(_.exclude("org.slf4j", "slf4j-log4j12"))
  )
}
