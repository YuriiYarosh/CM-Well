import mill._, scalalib._, scalafmt._
import coursier.maven.MavenRepository

object WorkerModule extends ZincWorkerModule {
  def repositories() = super.repositories ++ Seq(
    MavenRepository("https://oss.sonatype.org/content/repositories/releases")
  )
}

object Versions {
  val scala = "2.13.1"
  val akka = "2.5.26"
  val elastic = "7.4.2"
  val cassandra = ""
  val kafka = "2.4.0"
  val zookeeper = "3.5.6"
  val play = "2.7.3"
  val pac4j = "1.7.0"
  val gremlin = "2.6.0"

  val circe = "0.12.2"
  val metrics4 = "4.1.1"
  val bytecompressor = "1.2.2"
  val jena = "3.3.0"
  val slf4j = "1.3.70"
}

object CMWell extends ScalaModule with ScalafmtModule {
  def scalaVersion = "2.13.1"

  def scalacOptions = Seq("-Ydelambdafy:inline")

  def forkArgs = Seq("-Xmx8g")

  def zincWorker = CustomZincWorkerModule

  def mainClass = Some("foo.bar.Baz")

  def ivyDeps = Agg(
    ivy"ch.qos.logback:logback-classic:1.2.3",
    ivy"com.datastax.cassandra:cassandra-driver-core:3.7.1",
    ivy"com.ecyrd.speed4j:speed4j:0.18",
    ivy"com.fasterxml.jackson.core:jackson-core:2.10.0",
    ivy"com.github.andrewoma.dexx:collection:0.7",
    ivy"com.github.tomakehurst:wiremock:2.25.1",
    ivy"com.google.code.findbugs:jsr305:3.0.2",
    ivy"com.google.guava:guava:28.1-jre",
    ivy"com.ning:async-http-client:1.9.40",
    ivy"com.spatial4j:spatial4j:0.5",
    ivy"com.tinkerpop.blueprints:blueprints-core:${Versions.gremlin}", % gremlin_version,
    ivy"com.tinkerpop.gremlin:gremlin-groovy:{}", % gremlin_version,
    ivy"com.thaiopensource:jing:20091111",
    ivy"com.typesafe:config:1.4.0",
    ivy"commons-io:commons-io:2.6",
    ivy"commons-codec:commons-codec:1.13",
    ivy"commons-lang:commons-lang:2.6",
    ivy"com.jcraft:jsch:0.1.55",
    ivy"jakarta.ws.rs:jakarta.ws.rs-api:2.1.6",
    ivy"joda-time:joda-time:2.10.4",
    ivy"junit:junit:4.12",
    ivy"mx4j:mx4j-tools:3.0.1",
    ivy"org.lz4:lz4-java:1.6.0",
    ivy"net.lingala.zip4j:zip4j:2.2.3",
    ivy"net.sf.ehcache:ehcache:2.10.2",
    ivy"org.apache.cassandra:apache-cassandra:${Versions.cassandra}",
    ivy"org.apache.commons:commons-compress:1.19",
    ivy"org.apache.commons:commons-lang3:3.9",
    ivy"org.apache.commons:commons-csv:1.7",
    ivy"org.apache.httpcomponents:httpclient:4.5.10",
    ivy"org.apache.httpcomponents:httpcore:4.4.12",
    ivy"org.apache.abdera" % art % "1.1.3",
    ivy"org.apache.kafka:kafka-clients:${Versions.kafka}",
    ivy"org.apache.logging.log4j:log4j-to-slf4j:2.12.1",
    ivy"org.apache.tika:tika-core:1.22",
    ivy"org.apache.thrift:libthrift:0.9.3",
    ivy"org.apache.zookeeper:zookeeper:${Versions.zookeeper}",
    ivy"org.aspectj:aspectjweaver:1.8.9",
    ivy"org.codehaus.groovy:groovy-all:2.4.7",
    ivy"org.codehaus.plexus:plexus-archiver:4.2.0",
    ivy"org.codehaus.plexus:plexus-container-default:2.0.0",
    ivy"org.codehaus.plexus:plexus-utils:3.3.0",
    ivy"org.codehaus.woodstox:woodstox-asl:3.2.7",
    ivy"org.elasticsearch:elasticsearch:${Versions.elasticsearch}",
    ivy"org.elasticsearch.client:transport:${Versions.elasticsearch}",
    ivy"org.elasticsearch.distribution.zip:elasticsearch-oss:${Versions.elasticsearch}",
    ivy"org.elasticsearch:metrics-elasticsearch-reporter:2.2.0",
    ivy"org.hdrhistogram:HdrHistogram:2.1.11",
    ivy"org.jfarcand:wcs:1.3",
    ivy"org.jdom:jdom2:2.0.6",
    ivy"org.joda:joda-convert:2.2.1",
    ivy"org.openrdf.sesame:sesame-runtime:4.1.2",
    ivy"org.pac4j:play-pac4j_scala2.11:1.3.0",
    ivy"oivyrg.pac4j:pac4j-saml:${Versions.pac4j}",
    ivy"org.pac4j:pac4j-oauth:${Versions.pac4j}",
    ivy"org.pac4j:pac4j-openid:${Versions.pac4j}",
    ivy"org.slf4j:slf4j-api:${Versions.slf4j}",
    ivy"org.yaml:snakeyaml:1.25",
    ivy"xerces:xercesImpl:2.12.0",
    ivy"xml-apis:xml-apis:1.4.01",
    ivy"uk.org.lidalia:sysout-over-slf4j:1.0.2",
    ivy"org.xerial.snappy:snappy-java:1.1.2.4",
    ivy"io.netty:netty-all:4.1.42.Final",
    ivy"io.dropwizard.metrics:metrics-core:4.1.1",

    ivy"com.avast:bytecompressor:${Versions.bytecompressor}",
    ivy"com.avast:bytecompressor-huffman:${Versions.bytecompressor}",
    ivy"com.avast:bytecompressor-jsnappy:${Versions.bytecompressor}",
    ivy"com.avast:bytecompressor-zlib:${Versions.bytecompressor}",

    ivy"org.apache.kafka:kafka:${Versions.kafka}",

    ivy"org.apache.jena:apache-jena-libs:${Versions.jena}",

    ivy"com.github.t3hnar::scala-bcrypt:4.1",
    ivy"com.dimafeng::testcontainers-scala:0.33.0",
    ivy"com.lihaoyi::ujson:0.8.0",
    ivy"com.pauldijou:jwt-core:4.1.0",
    ivy"com.lightbend.akka::akka-stream-alpakka-cassandra:1.1.2",
    ivy"com.typesafe.scala-logging::scala-logging:3.9.2",
    ivy"com.typesafe.akka::akka-stream-kafka:1.1.0",
    ivy"com.typesafe.akka::akka-stream-contrib:0.10",
    ivy"com.typesafe.akka::akka-http:10.1.10",
    ivy"com.typesafe.akka::akka-actor:2.5.26",
    ivy"com.typesafe.play::twirl-api:1.3.13",
    ivy"com.typesafe.play::play-json:${Versions.play}",
    ivy"com.typesafe.play::play:${Versions.play}",
    ivy"com.twitter::chill-akka:0.5.2",
    ivy"eu.piotrbuda::scalawebsocket:0.1.1",
    ivy"io.circe:circe-core::${Versions.circe}",
    ivy"io.circe:circe-generic::${Versions.circe}",
    ivy"io.circe:circe-parser::${Versions.circe}",
    ivy"net.jcazevedo::moultingyaml:0.4.1",
    ivy"nl.grons::metrics4-scala:${Versions.metrics4}",
    ivy"nl.grons::metrics4-akka_a25:${Versions.metrics4}",
    ivy"nl.grons::metrics4-scala-hdr:${Versions.metrics4}",
    ivy"org.rogach::scallop:3.3.1",
    
    ivy"org.scala-lang.modules:scala-parser-combinators:1.0.4",
    ivy"org.scala-lang.modules:scala-xml:1.2.0",
    ivy"nl.gn0s1s::bump:0.1.3",

    ivy"org.scala-lang.modules::scala-collection-compat:2.1.3"
    ivy"org.scala-lang.modules::scala-library:${scalaVersion()}",
    ivy"org.scala-lang.modules::scala-reflect:${scalaVersion()}",
  )

  object test extends Tests {
    def ivyDeps = Agg(
      ivy"org.scalacheck::scalacheck:1.14.2",
      ivy"org.scalatest::scalatest:3.0.8",

      ivy"com.github.andrewoma.dexx:collection:0.7",
      
      ivy"com.typesafe.akka::akka-http::10.1.11",
      ivy"com.typesafe.akka::akka-stream::${Versions.akka}",
      
      ivy"org.slf4j:jcl-over-slf4j:1.5.11",
      ivy"org.slf4j:jul-to-slf4j:${Version.slf4j}",
      ivy"org.slf4j:log4j-over-slf4j:${Version.slf4j}",
    )

    def testFrameworks = Seq("org.scalatest.tools.Framework")
    def scalacOptions = CMWell.scalacOptions() ++ Seq(
      "-Yrangepos" 
    )
  }
}

def lineCount = T {
  CMWell.sources().flatMap(ref => os.walk(ref.path)).filter(_.isFile).flatMap(read.lines).size
}

def printLineCount() = T.command {
  println(lineCount())
}