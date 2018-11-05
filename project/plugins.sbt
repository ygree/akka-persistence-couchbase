addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.0.0") // for maintenance of copyright file header
addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.4.0") // sources autoformat

// whitesource for tracking licenses and vulnerabilities in dependencies
addSbtPlugin("com.lightbend" % "sbt-whitesource" % "0.1.13")

// pull in scala versions from the travis config and more
addSbtPlugin("com.dwijnand" % "sbt-travisci" % "1.1.3")
