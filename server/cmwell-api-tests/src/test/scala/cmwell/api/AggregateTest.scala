package cmwell.api

import java.util.stream.Collectors

import com.typesafe.config.ConfigList
import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration._

class AggregateTest extends Simulation {
  val urls = configuration.config.getList("urls")
    .stream().map { _.toString }
    .collect(Collectors.toList[String])


  val httpProto = http.baseUrls(urls)


}
