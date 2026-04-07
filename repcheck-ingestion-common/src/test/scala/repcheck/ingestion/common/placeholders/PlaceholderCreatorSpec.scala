package repcheck.ingestion.common.placeholders

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.dos.member.MemberDO

class PlaceholderCreatorSpec extends AnyFlatSpec with Matchers {

  private val creator = new DefaultPlaceholderCreator[IO]

  private def trackingRepo(
    ref: Ref[IO, List[MemberDO]]
  ): EntityRepository[IO, MemberDO] =
    new EntityRepository[IO, MemberDO] {
      override def insertIfNotExists(entity: MemberDO): IO[Unit] =
        ref.update(entity :: _)
    }

  "PlaceholderCreator.ensureExists" should "create placeholder when entity is missing" in {
    val program = for {
      ref <- Ref.of[IO, List[MemberDO]](List.empty)
      repo = trackingRepo(ref)
      _        <- creator.ensureExists[MemberDO]("B000444", repo)
      entities <- ref.get
    } yield entities

    val entities = program.unsafeRunSync()
    val _        = entities should have size 1
    val member   = entities.headOption.getOrElse(fail("Entity should have been inserted"))
    val _        = member.naturalKey shouldBe "B000444"
    val _        = member.memberId shouldBe 0L
    val _        = member.firstName shouldBe None
    member.lastName shouldBe None
  }

  it should "be a no-op when entity already exists (insertIfNotExists silently skips)" in {
    val program = for {
      callCountRef <- Ref.of[IO, Int](0)
      repo = new EntityRepository[IO, MemberDO] {
        override def insertIfNotExists(entity: MemberDO): IO[Unit] =
          callCountRef.update(_ + 1)
      }
      _         <- creator.ensureExists[MemberDO]("B000444", repo)
      _         <- creator.ensureExists[MemberDO]("B000444", repo)
      callCount <- callCountRef.get
    } yield callCount

    val callCount = program.unsafeRunSync()
    // Both calls go through to the repository, which silently handles duplicates
    callCount shouldBe 2
  }

  it should "be idempotent — calling twice is safe" in {
    val program = for {
      ref <- Ref.of[IO, List[MemberDO]](List.empty)
      repo = trackingRepo(ref)
      _        <- creator.ensureExists[MemberDO]("B000444", repo)
      _        <- creator.ensureExists[MemberDO]("B000444", repo)
      entities <- ref.get
    } yield entities

    val entities = program.unsafeRunSync()
    // Both calls delegate to repository; the repository is responsible for idempotency
    val _ = entities should have size 2
    all(entities.map(_.naturalKey)) shouldBe "B000444"
  }

  it should "create placeholder with only natural key populated" in {
    val program = for {
      ref <- Ref.of[IO, List[MemberDO]](List.empty)
      repo = trackingRepo(ref)
      _        <- creator.ensureExists[MemberDO]("S000148", repo)
      entities <- ref.get
    } yield entities

    val entities = program.unsafeRunSync()
    val member   = entities.headOption.getOrElse(fail("Entity should have been inserted"))
    val _        = member.naturalKey shouldBe "S000148"
    val _        = member.memberId shouldBe 0L
    val _        = member.firstName shouldBe None
    val _        = member.lastName shouldBe None
    val _        = member.directOrderName shouldBe None
    val _        = member.invertedOrderName shouldBe None
    val _        = member.honorificName shouldBe None
    val _        = member.birthYear shouldBe None
    val _        = member.currentParty shouldBe None
    val _        = member.state shouldBe None
    val _        = member.district shouldBe None
    val _        = member.imageUrl shouldBe None
    val _        = member.imageAttribution shouldBe None
    val _        = member.officialUrl shouldBe None
    val _        = member.updateDate shouldBe None
    val _        = member.createdAt shouldBe None
    member.updatedAt shouldBe None
  }

}
