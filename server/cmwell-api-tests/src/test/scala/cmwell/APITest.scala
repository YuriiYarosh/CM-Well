package cmwell

import io.gatling.core.Predef._
import io.gatling.core.scenario.Simulation
import io.gatling.http.Predef._

object APITest {
  val url = configuration.config.getString("gatling.base-url")
  val acceptHeader = configuration.config.getString("gatling.accept-header")
  val userAgent = configuration.config.getString("gatling.user-agent")

  val httpProtocol = http.baseUrl(url)
    .acceptHeader(acceptHeader) // 6
    .doNotTrackHeader("1")
    .acceptLanguageHeader("en-US,en;q=0.5")
    .acceptEncodingHeader("gzip, deflate")
    .userAgentHeader(userAgent)
}

abstract class APITest extends Simulation {
  abstract val persistedFixtures: List[PersistedFixture[_]]
}
