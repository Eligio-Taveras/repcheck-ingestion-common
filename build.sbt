import org.typelevel.scalacoptions.ScalacOption
import sbt.Keys.libraryDependencies
import sbt.Def
import Dependencies.*
import com.repcheck.sbt.ExceptionUniquenessPlugin.autoImport.exceptionUniquenessRootPackages

val isScala212: Def.Initialize[Boolean] = Def.setting {
  VersionNumber(scalaVersion.value).matchesSemVer(SemanticSelector("2.12.x"))
}

ThisBuild / dynverSonatypeSnapshots := true

lazy val commonSettings = Seq(
  organization := "com.repcheck",
  scalaVersion := "3.7.3",
  publishTo := Some(
    "GitHub Packages" at s"https://maven.pkg.github.com/Eligio-Taveras/repcheck-ingestion-common"
  ),
  publishMavenStyle := true,
  credentials ++= {
    val envCreds = for {
      user  <- sys.env.get("GITHUB_ACTOR")
      token <- sys.env.get("GITHUB_TOKEN")
    } yield Credentials("GitHub Package Registry", "maven.pkg.github.com", user, token)

    val fileCreds = {
      val f = Path.userHome / ".sbt" / ".github-packages-credentials"
      if (f.exists) Some(Credentials(f)) else None
    }

    envCreds.orElse(fileCreds).toSeq
  },
  resolvers ++= Seq(
    "GitHub Packages - shared-models" at "https://maven.pkg.github.com/Eligio-Taveras/repcheck-shared-models",
    "GitHub Packages - pipeline-models" at "https://maven.pkg.github.com/Eligio-Taveras/repcheck-pipeline-models",
    "GitHub Packages - db-migrations" at "https://maven.pkg.github.com/Eligio-Taveras/repcheck-db-migrations",
  ),
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % "3.2.18" % Test
  ),
  semanticdbEnabled := true,
  tpolecatScalacOptions ++= ScalaCConfig.scalaCOptions,
  tpolecatScalacOptions ++= {
    if (isScala212.value) ScalaCConfig.scalaCOption2_12
    else Set.empty[ScalacOption]
  },

  // WartRemover — enforces FP discipline at compile time
  wartremoverErrors ++= Seq(
    Wart.AsInstanceOf,          // No unsafe casts
    Wart.EitherProjectionPartial, // No .get on Either projections
    Wart.IsInstanceOf,          // No runtime type checks — use pattern matching
    Wart.MutableDataStructures, // No mutable collections
    Wart.Null,                  // No null — use Option
    Wart.OptionPartial,         // No Option.get — use fold/map/getOrElse
    Wart.Return,                // No return statements
    Wart.StringPlusAny,         // No string + any — use interpolation
    Wart.IterableOps,           // No .head/.tail on collections — use headOption
    Wart.TryPartial,            // No Try.get — use fold/recover
    Wart.Var                    // No mutable vars
  ),
  wartremoverWarnings ++= Seq(
    Wart.Throw                  // Warn on bare throw — prefer F.raiseError
  )
)

lazy val root = (project in file("."))
  .aggregate(repcheckingestioncommon, docGenerator)
  .settings(
    commonSettings,
    name := "repcheck-ingestion-common-root",
    publish / skip := true
  )

lazy val repcheckingestioncommon = (project in file("repcheck-ingestion-common"))
  .enablePlugins(com.repcheck.sbt.ExceptionUniquenessPlugin)
  .settings(
    commonSettings,
    name := "repcheck-ingestion-common",
    exceptionUniquenessRootPackages := Seq("com.repcheck", "repcheck"),
    libraryDependencies ++= http4sEmber ++ circe ++ pureConfig ++ fs2
      ++ catsEffect ++ testDeps
      ++ doobie
      ++ pubSub
      ++ xml
      ++ logging
      ++ diff
    ,
    libraryDependencies += "com.h2database" % "h2" % "2.2.224" % Test,
    libraryDependencies += "com.repcheck" %% "repchecksharedmodels" % "0.1.16",
    libraryDependencies += "com.repcheck" %% "repcheck-pipeline-models" % "0.1.16",
    libraryDependencies += "com.repcheck" %% "repcheck-db-migrations-runner" % "0.1.9" % Test,
    // Circe semi-auto derivation for large case classes
    scalacOptions += "-Xmax-inlines:64",
    // DB-backed suites share a single AlloyDB Omni container and TRUNCATE shared tables in
    // beforeEach/withRepo. Parallel execution causes cross-suite TRUNCATE races where one
    // suite wipes another's just-inserted rows.
    Test / parallelExecution := false,
    // Exclude DB-backed integration tests from `sbt test` by default — they require a local
    // Docker daemon to start an AlloyDB Omni container. Use the `dockerTest` alias below to
    // run them explicitly.
    Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-l", "DockerRequired")
  )

lazy val docGenerator = (project in file("doc-generator"))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "com.anthropic" % "anthropic-java" % "2.18.0",
      "org.typelevel" %% "cats-effect" % "3.7.0",
      "ch.qos.logback" % "logback-classic" % "1.5.32"
    ),
    // Exclude WartRemover for this utility project — uses Java SDK patterns
    wartremoverErrors := Seq.empty,
    wartremoverWarnings := Seq.empty,
    // Exclude from coverage — utility project with no unit tests
    coverageEnabled := false
  )

// `dockerTest` runs only the DB-backed integration tests against a local AlloyDB Omni
// container. The default `Test / testOptions` exclude DockerRequired tests, so this alias
// overrides those options for the duration of the run and then restores them.
addCommandAlias(
  "dockerTest",
  "; set repcheckingestioncommon / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-n\", \"DockerRequired\"))" +
    "; repcheckingestioncommon / test" +
    "; set repcheckingestioncommon / Test / testOptions := Seq(Tests.Argument(TestFrameworks.ScalaTest, \"-l\", \"DockerRequired\"))",
)
