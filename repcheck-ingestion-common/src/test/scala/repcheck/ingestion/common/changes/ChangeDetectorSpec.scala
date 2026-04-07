package repcheck.ingestion.common.changes

import java.time.Instant

import difflicious.Differ
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class SimpleEntity(name: String, value: Int, updateDate: Option[String])

case class NestedChild(firstName: String, lastName: String)

case class NestedParent(name: String, child: NestedChild)

object ChangeDetectorSpec {
  given Differ[SimpleEntity] = Differ.derived
  given Differ[NestedChild]  = Differ.derived
  given Differ[NestedParent] = Differ.derived
}

class ChangeDetectorSpec extends AnyFlatSpec with Matchers {
  import ChangeDetectorSpec.given

  private val baseTime    = Instant.parse("2024-01-01T00:00:00Z")
  private val laterTime   = Instant.parse("2024-01-02T00:00:00Z")
  private val earlierTime = Instant.parse("2023-12-31T00:00:00Z")

  "ChangeDetector" should "return New when stored is None" in {
    val incoming = SimpleEntity("bill-1", 42, Some("2024-01-01"))
    val result = ChangeDetector.detect(
      incoming,
      stored = None,
      incomingUpdateDate = baseTime,
      storedUpdateDate = None,
    )
    result shouldBe ChangeReport.New(incoming)
  }

  it should "return Updated with both sides and a diff when incoming is newer and fields differ" in {
    val incoming = SimpleEntity("bill-1-updated", 42, Some("2024-01-01"))
    val stored   = SimpleEntity("bill-1", 42, Some("2024-01-01"))
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result match {
      case ChangeReport.Updated(i, s, diff) =>
        val _ = i shouldBe incoming
        val _ = s shouldBe stored
        diff.isOk shouldBe false
      case other => fail(s"expected Updated, got $other")
    }
  }

  it should "return Unchanged when incoming is newer but structurally equal" in {
    val entity = SimpleEntity("bill-1", 42, Some("2024-01-01"))
    val result = ChangeDetector.detect(
      entity,
      stored = Some(entity),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result shouldBe ChangeReport.Unchanged()
  }

  it should "return Unchanged when timestamps are equal (never overwrite)" in {
    val incoming = SimpleEntity("bill-1-updated", 42, Some("2024-01-01"))
    val stored   = SimpleEntity("bill-1", 42, Some("2024-01-01"))
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = baseTime,
      storedUpdateDate = Some(baseTime),
    )
    result shouldBe ChangeReport.Unchanged()
  }

  it should "return Unchanged when incoming is older than stored (data regression)" in {
    val incoming = SimpleEntity("bill-1-old", 42, Some("2024-01-01"))
    val stored   = SimpleEntity("bill-1", 42, Some("2024-01-01"))
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = earlierTime,
      storedUpdateDate = Some(baseTime),
    )
    result shouldBe ChangeReport.Unchanged()
  }

  it should "return Unchanged when no stored update date is present and entities are equal" in {
    val entity = SimpleEntity("bill-1", 42, Some("2024-01-01"))
    val result = ChangeDetector.detect(
      entity,
      stored = Some(entity),
      incomingUpdateDate = baseTime,
      storedUpdateDate = None,
    )
    result shouldBe ChangeReport.Unchanged()
  }

  it should "return Updated when no stored update date is present and entities differ" in {
    val incoming = SimpleEntity("bill-1-updated", 42, Some("2024-01-01"))
    val stored   = SimpleEntity("bill-1", 42, Some("2024-01-01"))
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = baseTime,
      storedUpdateDate = None,
    )
    result match {
      case ChangeReport.Updated(i, s, diff) =>
        val _ = i shouldBe incoming
        val _ = s shouldBe stored
        diff.isOk shouldBe false
      case other => fail(s"expected Updated, got $other")
    }
  }

  it should "produce a non-ok diff for nested case-class differences" in {
    val incoming = NestedParent("parent", NestedChild("John", "Smith"))
    val stored   = NestedParent("parent", NestedChild("John", "Doe"))
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result match {
      case ChangeReport.Updated(i, s, diff) =>
        val _ = i shouldBe incoming
        val _ = s shouldBe stored
        diff.isOk shouldBe false
      case other => fail(s"expected Updated, got $other")
    }
  }

  it should "treat nested case classes as Unchanged when structurally equal" in {
    val entity = NestedParent("parent", NestedChild("John", "Smith"))
    val result = ChangeDetector.detect(
      entity,
      stored = Some(entity),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result shouldBe ChangeReport.Unchanged()
  }

}
