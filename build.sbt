lazy val buildSettings = Seq(
    organization := "com.github.finagle",
    version := "0.1.0",
    scalaVersion := "2.12.11",
    crossScalaVersions := Seq("2.12.11", "2.13.2")
)

val baseSettings = Seq(
    libraryDependencies ++= Seq(
        "com.twitter" %% "finagle-core" % "20.5.0",
        "io.rsocket" % "rsocket-core" % "1.0.1",
        "io.rsocket" % "rsocket-transport-netty" % "1.0.1",
        "org.scalacheck" %% "scalacheck" % "1.14.3" % Test,
        "org.scalatest" %% "scalatest" % "3.1.2" % Test
    )
)

lazy val publishSettings = Seq(
    publishMavenStyle := true,
    publishArtifact := true,
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    publishArtifact in Test := false,
    licenses := Seq(
        "Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")
    ),
    homepage := Some(url("https://github.com/finagle/finagle-oauth2")),
    autoAPIMappings := true,
    scmInfo := Some(
        ScmInfo(
            url("https://github.com/finagle/finagle-rsocket"),
            "scm:git:git@github.com:finagle/finagle-rsocket.git"
        )
    ),
    pomExtra :=
      <developers>
      <developer>
        <id>finagle</id>
        <name>Finagle OSS</name>
        <url>https://twitter.com/finagle</url>
      </developer>
    </developers>
)

lazy val allSettings = baseSettings ++ buildSettings ++ publishSettings

lazy val rsocket = project
  .in(file("."))
  .settings(moduleName := "finagle-rsocket")
  .settings(allSettings)
