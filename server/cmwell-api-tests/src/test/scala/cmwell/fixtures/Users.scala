package cmwell.fixtures

import cmwell.PersistedFixture

case class CMWellUser()

class Users extends PersistedFixture[List[CMWellUser]] {
  override def apply(): List[CMWellUser] = {
    List(CMWellUser())
  }

  override def persist() = {

  }

  override def purge() = {

  }
}
