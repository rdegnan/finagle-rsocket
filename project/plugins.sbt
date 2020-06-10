resolvers ++= Seq(
    Classpaths.typesafeReleases,
    Classpaths.sbtPluginReleases
)

addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.2")
