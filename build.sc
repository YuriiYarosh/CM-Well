import mill._, scalalib._, scalafmt._
import coursier.maven.MavenRepository

object WorkerModule extends ZincWorkerModule {
  def repositories() = super.repositories ++ Seq(
    MavenRepository("https://oss.sonatype.org/content/repositories/releases")
  )
}

object CMWell extends ScalaModule with ScalafmtModule {
  def scalaVersion = "2.13.1"

  def scalacOptions = Seq("-Ydelambdafy:inline")

  def forkArgs = Seq("-Xmx8g")

  def zincWorker = CustomZincWorkerModule

  def mainClass = Some("foo.bar.Baz")

  def ivyDeps = Agg(
    ivy"com.lihaoyi::upickle:0.5.1",
    ivy"com.lihaoyi::pprint:0.5.2",
    ivy"com.lihaoyi::fansi:0.2.4",
    ivy"${scalaOrganization()}:scala-reflect:${scalaVersion()}"
  )

  object test extends Tests {
    def ivyDeps = Agg(ivy"org.scalatest::scalatest:3.0.4")
    def testFrameworks = Seq("org.scalatest.tools.Framework")
    def scalacOptions = CMWell.scalacOptions() ++ Seq(
      "-Yrangepos" 
    )
  }

  object integration extends Tests {
    def ivyDeps = Agg(ivy"org.scalatest::scalatest:3.0.4")
    def testFrameworks = Seq("org.scalatest.tools.Framework")
    def scalacOptions = test.scalacOptions() 
  }
}

def lineCount = T {
  CMWell.sources().flatMap(ref => os.walk(ref.path)).filter(_.isFile).flatMap(read.lines).size
}

def printLineCount() = T.command {
  println(lineCount())
}