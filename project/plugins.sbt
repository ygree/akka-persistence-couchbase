addSbtPlugin("de.heikoseeberger" % "sbt-header" % "5.0.0") // for maintenance of copyright file header
addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.4.0") // sources autoformat

// whitesource for tracking licenses and vulnerabilities in dependencies
addSbtPlugin("com.lightbend" % "sbt-whitesource" % "0.1.13")

// pull in scala versions from the travis config and more
addSbtPlugin("com.dwijnand" % "sbt-travisci" % "1.1.3")

// for releasing
addSbtPlugin("com.dwijnand" % "sbt-dynver" % "3.0.0")
addSbtPlugin("org.foundweekends" % "sbt-bintray" % "0.5.2")

addSbtPlugin("com.typesafe.sbt" % "sbt-multi-jvm" % "0.4.0")

// docs
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox" % "0.4.2")
addSbtPlugin("com.lightbend.akka" % "sbt-paradox-akka" % "0.14")
