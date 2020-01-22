resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayRepo("slamdata-inc", "maven-public")

addSbtPlugin("com.slamdata" % "sbt-slamdata" % "5.2.0")
addSbtPlugin("com.slamdata" % "sbt-quasar-plugin" % "0.2.3")
