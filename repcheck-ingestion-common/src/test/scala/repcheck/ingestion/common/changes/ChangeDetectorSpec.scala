package repcheck.ingestion.common.changes

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class SimpleEntity(name: String, value: Int, updateDate: Option[String])

case class NestedChild(firstName: String, lastName: String)

case class NestedParent(name: String, child: NestedChild)

case class ListItem(id: String, label: String) extends HasNaturalKey {
  def naturalKey: String = id
}

case class ListParent(name: String, items: List[ListItem])

case class PlainListParent(name: String, tags: List[String])

class ChangeDetectorSpec extends AnyFlatSpec with Matchers {

  private val baseTime    = Instant.parse("2024-01-01T00:00:00Z")
  private val laterTime   = Instant.parse("2024-01-02T00:00:00Z")
  private val earlierTime = Instant.parse("2023-12-31T00:00:00Z")

  // Test 1: stored = None -> New(incoming)
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

  // Test 2: incoming > stored with field changes -> Updated(diffs)
  it should "return Updated when incoming is newer and fields differ" in {
    val incoming = SimpleEntity("bill-1-updated", 42, Some("2024-01-01"))
    val stored   = SimpleEntity("bill-1", 42, Some("2024-01-01"))
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result match {
      case ChangeReport.Updated(diffs) =>
        diffs should have size 1
        diffs.headOption.map(_.fieldName) shouldBe Some("name")
        diffs.headOption.map(_.oldValue) shouldBe Some("bill-1")
        diffs.headOption.map(_.newValue) shouldBe Some("bill-1-updated")
      case other =>
        fail(s"Expected Updated but got $other")
    }
  }

  // Test 3: incoming > stored with no field changes -> Unchanged
  it should "return Unchanged when incoming is newer but fields are identical" in {
    val entity = SimpleEntity("bill-1", 42, Some("2024-01-01"))
    val result = ChangeDetector.detect(
      entity,
      stored = Some(entity),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result shouldBe a[ChangeReport.Unchanged[?]]
  }

  // Test 4: incoming == stored -> Unchanged
  it should "return Unchanged when timestamps are equal" in {
    val entity = SimpleEntity("bill-1", 42, Some("2024-01-01"))
    val result = ChangeDetector.detect(
      entity,
      stored = Some(entity),
      incomingUpdateDate = baseTime,
      storedUpdateDate = Some(baseTime),
    )
    result shouldBe a[ChangeReport.Unchanged[?]]
  }

  // Test 5: incoming < stored -> Unchanged (data regression)
  it should "return Unchanged when incoming is older than stored (data regression)" in {
    val incoming = SimpleEntity("bill-1-old", 42, Some("2024-01-01"))
    val stored   = SimpleEntity("bill-1", 42, Some("2024-01-01"))
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = earlierTime,
      storedUpdateDate = Some(baseTime),
    )
    result shouldBe a[ChangeReport.Unchanged[?]]
  }

  // Test 6: Nested case class diffing
  it should "report nested field diffs with dot notation" in {
    val incoming = NestedParent("parent", NestedChild("John", "Smith"))
    val stored   = NestedParent("parent", NestedChild("John", "Doe"))
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result match {
      case ChangeReport.Updated(diffs) =>
        diffs should have size 1
        diffs.headOption.map(_.fieldName) shouldBe Some("child.lastName")
        diffs.headOption.map(_.oldValue) shouldBe Some("Doe")
        diffs.headOption.map(_.newValue) shouldBe Some("Smith")
      case other =>
        fail(s"Expected Updated but got $other")
    }
  }

  // Test 7: List addition detected by natural key
  it should "detect list item additions by natural key" in {
    val incoming = ListParent(
      "parent",
      List(ListItem("A001", "Alice"), ListItem("B002", "Bob")),
    )
    val stored = ListParent("parent", List(ListItem("A001", "Alice")))
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result match {
      case ChangeReport.Updated(diffs) =>
        diffs.exists(_.fieldName == "items[+B002]") shouldBe true
      case other =>
        fail(s"Expected Updated but got $other")
    }
  }

  // Test 8: List removal detected by natural key
  it should "detect list item removals by natural key" in {
    val incoming = ListParent("parent", List(ListItem("A001", "Alice")))
    val stored = ListParent(
      "parent",
      List(ListItem("A001", "Alice"), ListItem("B002", "Bob")),
    )
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result match {
      case ChangeReport.Updated(diffs) =>
        diffs.exists(_.fieldName == "items[-B002]") shouldBe true
      case other =>
        fail(s"Expected Updated but got $other")
    }
  }

  // Test 9: List item modification detected
  it should "detect list item field changes with keyed notation" in {
    val incoming = ListParent(
      "parent",
      List(ListItem("A001", "Alice Updated")),
    )
    val stored = ListParent("parent", List(ListItem("A001", "Alice")))
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result match {
      case ChangeReport.Updated(diffs) =>
        diffs.exists(_.fieldName == "items[A001].label") shouldBe true
      case other =>
        fail(s"Expected Updated but got $other")
    }
  }

  // Test 10: List ordering does not affect comparison
  it should "not report diffs when list items are reordered" in {
    val incoming = ListParent(
      "parent",
      List(ListItem("B002", "Bob"), ListItem("A001", "Alice")),
    )
    val stored = ListParent(
      "parent",
      List(ListItem("A001", "Alice"), ListItem("B002", "Bob")),
    )
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result shouldBe a[ChangeReport.Unchanged[?]]
  }

  // Test 11: Placeholder vs full entity produces diffs
  it should "detect diffs between placeholder and full entity" in {
    val placeholder = SimpleEntity("", 0, None)
    val full        = SimpleEntity("Full Title", 119, Some("2024-01-15"))
    val result = ChangeDetector.detect(
      full,
      stored = Some(placeholder),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result match {
      case ChangeReport.Updated(diffs) =>
        diffs.size should be >= 2
        diffs.exists(_.fieldName == "name") shouldBe true
        diffs.exists(_.fieldName == "value") shouldBe true
      case other =>
        fail(s"Expected Updated but got $other")
    }
  }

  // Additional: unkeyed list comparison ignores ordering
  it should "not report diffs for unkeyed lists with same elements in different order" in {
    val incoming = PlainListParent("parent", List("beta", "alpha"))
    val stored   = PlainListParent("parent", List("alpha", "beta"))
    val result = ChangeDetector.detect(
      incoming,
      stored = Some(stored),
      incomingUpdateDate = laterTime,
      storedUpdateDate = Some(baseTime),
    )
    result shouldBe a[ChangeReport.Unchanged[?]]
  }

}
