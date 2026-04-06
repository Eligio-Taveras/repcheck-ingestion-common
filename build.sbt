import org.typelevel.scalacoptions.ScalacOption
import sbt.Keys.libraryDependencies
import sbt.Def
import Dependencies.*

val isScala212: Def.Initialize[Boolean] = Def.setting {
  VersionNumber(scalaVersion.value).matchesSemVer(SemanticSelector("2.12.x"))
}

ThisBuild / dynverSonatypeSnapshots := true

lazy val commonSettings = Seq(
  organization := "com.repcheck",
  scalaVersion := "3.4.1",
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
  .settings(
    commonSettings,
    name := "repcheck-ingestion-common",
    libraryDependencies ++= http4sEmber ++ circe ++ pureConfig ++ fs2
      ++ catsEffect ++ testDeps
      ++ doobie
      ++ pubSub
    ,
    libraryDependencies += "com.h2database" % "h2" % "2.2.224" % Test,
    libraryDependencies += "com.repcheck" %% "repchecksharedmodels" % "0.1.2",
    libraryDependencies += "com.repcheck" %% "repcheck-pipeline-models" % "0.1.3",
    // Circe semi-auto derivation for large case classes
    scalacOptions += "-Xmax-inlines:64"
  )

lazy val docGenerator = (project in file("doc-generator"))
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "com.anthropic" % "anthropic-java" % "2.18.0",
      "org.typelevel" %% "cats-effect" % "3.5.4",
      "ch.qos.logback" % "logback-classic" % "1.5.6"
    ),
    // Exclude WartRemover for this utility project — uses Java SDK patterns
    wartremoverErrors := Seq.empty,
    wartremoverWarnings := Seq.empty,
    // Exclude from coverage — utility project with no unit tests
    coverageEnabled := false
  )
