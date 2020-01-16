package cmwell

import io.gatling.http.protocol.HttpProtocolBuilder

trait Fixture[T] {
  implicit val httpProtocol: HttpProtocolBuilder = APITest.httpProtocol

  def apply(): T
}

trait PersistedFixture[A] extends Fixture[A] {
  def persist()
  def purge()
}