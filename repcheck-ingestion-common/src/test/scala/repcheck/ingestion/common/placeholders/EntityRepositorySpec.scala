package repcheck.ingestion.common.placeholders

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.member.MemberDO
import repcheck.shared.models.placeholder.HasPlaceholder

class EntityRepositorySpec extends AnyFlatSpec with Matchers {

  private def conflictAwareRepo(
    storeRef: Ref[IO, Map[String, MemberDO]],
    callCountRef: Ref[IO, Int],
  ): EntityRepository[IO, MemberDO] =
    new EntityRepository[IO, MemberDO] {
      override def insertIfNotExists(entity: MemberDO): IO[Unit] =
        for {
          _ <- callCountRef.update(_ + 1)
          _ <- storeRef.update { store =>
            if (store.contains(entity.naturalKey)) {
              store // ON CONFLICT DO NOTHING
            } else {
              store + (entity.naturalKey -> entity)
            }
          }
        } yield ()
    }

  "EntityRepository.insertIfNotExists" should "insert a new entity" in {
    val program = for {
      storeRef     <- Ref.of[IO, Map[String, MemberDO]](Map.empty)
      callCountRef <- Ref.of[IO, Int](0)
      repo        = conflictAwareRepo(storeRef, callCountRef)
      placeholder = HasPlaceholder[MemberDO].placeholder("B000444")
      _     <- repo.insertIfNotExists(placeholder)
      store <- storeRef.get
    } yield store

    val store = program.unsafeRunSync()
    val _     = store should contain key "B000444"
    store("B000444").naturalKey shouldBe "B000444"
  }

  it should "silently skip on conflict (entity already exists)" in {
    val program = for {
      storeRef     <- Ref.of[IO, Map[String, MemberDO]](Map.empty)
      callCountRef <- Ref.of[IO, Int](0)
      repo        = conflictAwareRepo(storeRef, callCountRef)
      placeholder = HasPlaceholder[MemberDO].placeholder("B000444")
      _         <- repo.insertIfNotExists(placeholder)
      _         <- repo.insertIfNotExists(placeholder)
      store     <- storeRef.get
      callCount <- callCountRef.get
    } yield (store, callCount)

    val (store, callCount) = program.unsafeRunSync()
    val _                  = callCount shouldBe 2
    val _                  = store should have size 1
    store("B000444").naturalKey shouldBe "B000444"
  }

}
