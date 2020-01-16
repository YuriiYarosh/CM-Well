package cmwell.api

import cmwell.fixtures.Users
import cmwell.{APITest, PersistedFixture}

object AuthTest extends APITest {
  override val persistedFixtures: List[PersistedFixture[_]] = List(
    new Users()
  )
}

class AuthTest {

}
