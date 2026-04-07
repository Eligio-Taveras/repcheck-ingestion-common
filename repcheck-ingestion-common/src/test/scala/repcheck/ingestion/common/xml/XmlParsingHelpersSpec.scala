package repcheck.ingestion.common.xml

import java.time.format.DateTimeFormatter
import java.time.{Instant, LocalDate}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.common.errors.XmlFieldMissing

class XmlParsingHelpersSpec extends AnyFlatSpec with Matchers {

  private val isoDate: DateTimeFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  "text" should "return value for present tag" in {
    val xml = <root><name>John</name></root>
    XmlParsingHelpers.text(xml, "name") shouldBe Right("John")
  }

  it should "return Left(XmlFieldMissing) for absent tag" in {
    val xml    = <root></root>
    val result = XmlParsingHelpers.text(xml, "name")
    val _      = result.isLeft shouldBe true
    result.fold(identity, _ => fail("expected Left but got Right")) shouldBe a[XmlFieldMissing]
  }

  it should "return Left(XmlFieldMissing) for empty tag" in {
    val xml    = <root><name></name></root>
    val result = XmlParsingHelpers.text(xml, "name")
    result.isLeft shouldBe true
  }

  it should "trim whitespace" in {
    val xml = <root><name>  John  </name></root>
    XmlParsingHelpers.text(xml, "name") shouldBe Right("John")
  }

  "textOpt" should "return Some for present tag" in {
    val xml = <root><name>John</name></root>
    XmlParsingHelpers.textOpt(xml, "name") shouldBe Some("John")
  }

  it should "return None for absent tag" in {
    val xml = <root></root>
    XmlParsingHelpers.textOpt(xml, "name") shouldBe None
  }

  it should "return None for empty tag" in {
    val xml = <root><name></name></root>
    XmlParsingHelpers.textOpt(xml, "name") shouldBe None
  }

  it should "return None for whitespace-only tag" in {
    val xml = <root><name>   </name></root>
    XmlParsingHelpers.textOpt(xml, "name") shouldBe None
  }

  "int" should "parse valid integers" in {
    val xml = <root><count>42</count></root>
    XmlParsingHelpers.int(xml, "count") shouldBe Right(42)
  }

  it should "return Left for non-integer text" in {
    val xml    = <root><count>abc</count></root>
    val result = XmlParsingHelpers.int(xml, "count")
    result.isLeft shouldBe true
  }

  it should "return Left for absent tag" in {
    val xml    = <root></root>
    val result = XmlParsingHelpers.int(xml, "count")
    result.isLeft shouldBe true
  }

  it should "parse negative integers" in {
    val xml = <root><count>-5</count></root>
    XmlParsingHelpers.int(xml, "count") shouldBe Right(-5)
  }

  "intOpt" should "return Some for valid integer" in {
    val xml = <root><count>42</count></root>
    XmlParsingHelpers.intOpt(xml, "count") shouldBe Some(42)
  }

  it should "return None for absent tag" in {
    val xml = <root></root>
    XmlParsingHelpers.intOpt(xml, "count") shouldBe None
  }

  it should "return None for non-integer text" in {
    val xml = <root><count>abc</count></root>
    XmlParsingHelpers.intOpt(xml, "count") shouldBe None
  }

  "dateOpt" should "parse dates with given formatter" in {
    val xml    = <root><date>2024-01-15</date></root>
    val result = XmlParsingHelpers.dateOpt(xml, "date", isoDate)
    result shouldBe Some(LocalDate.of(2024, 1, 15))
  }

  it should "return None for absent tag" in {
    val xml = <root></root>
    XmlParsingHelpers.dateOpt(xml, "date", isoDate) shouldBe None
  }

  it should "return None for invalid date" in {
    val xml = <root><date>not-a-date</date></root>
    XmlParsingHelpers.dateOpt(xml, "date", isoDate) shouldBe None
  }

  it should "parse dates with custom formatter" in {
    val formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy")
    val xml       = <root><date>01/15/2024</date></root>
    val result    = XmlParsingHelpers.dateOpt(xml, "date", formatter)
    result shouldBe Some(LocalDate.of(2024, 1, 15))
  }

  "children" should "return matching child nodes" in {
    val xml    = <root><member>A</member><member>B</member><member>C</member></root>
    val result = XmlParsingHelpers.children(xml, "member")
    result.length shouldBe 3
  }

  it should "return empty seq when no matching children" in {
    val xml    = <root><other>A</other></root>
    val result = XmlParsingHelpers.children(xml, "member")
    result shouldBe empty
  }

  it should "return empty seq for empty node" in {
    val xml    = <root></root>
    val result = XmlParsingHelpers.children(xml, "member")
    result shouldBe empty
  }

  // --- Long helpers ---

  "long" should "parse valid long values" in {
    val xml = <root><identifier>1191202517</identifier></root>
    XmlParsingHelpers.long(xml, "identifier") shouldBe Right(1191202517L)
  }

  it should "return Left for non-numeric text" in {
    val xml = <root><identifier>abc</identifier></root>
    XmlParsingHelpers.long(xml, "identifier").isLeft shouldBe true
  }

  it should "return Left for absent tag" in {
    val xml = <root></root>
    XmlParsingHelpers.long(xml, "identifier").isLeft shouldBe true
  }

  "longOpt" should "return Some for valid long" in {
    val xml = <root><identifier>1191202517</identifier></root>
    XmlParsingHelpers.longOpt(xml, "identifier") shouldBe Some(1191202517L)
  }

  it should "return None for absent tag" in {
    val xml = <root></root>
    XmlParsingHelpers.longOpt(xml, "identifier") shouldBe None
  }

  it should "return None for non-numeric text" in {
    val xml = <root><identifier>abc</identifier></root>
    XmlParsingHelpers.longOpt(xml, "identifier") shouldBe None
  }

  // --- Boolean helpers ---

  "bool" should "parse true" in {
    val xml = <root><isOriginalCosponsor>true</isOriginalCosponsor></root>
    XmlParsingHelpers.bool(xml, "isOriginalCosponsor") shouldBe Right(true)
  }

  it should "parse false" in {
    val xml = <root><isCivilian>false</isCivilian></root>
    XmlParsingHelpers.bool(xml, "isCivilian") shouldBe Right(false)
  }

  it should "return Left for non-boolean text" in {
    val xml = <root><isOriginalCosponsor>yes</isOriginalCosponsor></root>
    XmlParsingHelpers.bool(xml, "isOriginalCosponsor").isLeft shouldBe true
  }

  it should "return Left for absent tag" in {
    val xml = <root></root>
    XmlParsingHelpers.bool(xml, "isOriginalCosponsor").isLeft shouldBe true
  }

  "boolOpt" should "return Some(true) for true" in {
    val xml = <root><isCurrent>true</isCurrent></root>
    XmlParsingHelpers.boolOpt(xml, "isCurrent") shouldBe Some(true)
  }

  it should "return Some(false) for false" in {
    val xml = <root><isCurrent>false</isCurrent></root>
    XmlParsingHelpers.boolOpt(xml, "isCurrent") shouldBe Some(false)
  }

  it should "return None for absent tag" in {
    val xml = <root></root>
    XmlParsingHelpers.boolOpt(xml, "isCurrent") shouldBe None
  }

  it should "return None for non-boolean text" in {
    val xml = <root><isCurrent>maybe</isCurrent></root>
    XmlParsingHelpers.boolOpt(xml, "isCurrent") shouldBe None
  }

  // --- Instant helpers ---

  "instant" should "parse UTC instant" in {
    val xml = <root><updateDate>2025-05-27T14:16:54Z</updateDate></root>
    XmlParsingHelpers.instant(xml, "updateDate") shouldBe Right(
      Instant.parse("2025-05-27T14:16:54Z")
    )
  }

  it should "return Left for invalid instant" in {
    val xml = <root><updateDate>not-a-date</updateDate></root>
    XmlParsingHelpers.instant(xml, "updateDate").isLeft shouldBe true
  }

  it should "return Left for absent tag" in {
    val xml = <root></root>
    XmlParsingHelpers.instant(xml, "updateDate").isLeft shouldBe true
  }

  "instantOpt" should "return Some for valid UTC instant" in {
    val xml = <root><updateDate>2025-05-27T14:16:54Z</updateDate></root>
    XmlParsingHelpers.instantOpt(xml, "updateDate") shouldBe Some(
      Instant.parse("2025-05-27T14:16:54Z")
    )
  }

  it should "return None for absent tag" in {
    val xml = <root></root>
    XmlParsingHelpers.instantOpt(xml, "updateDate") shouldBe None
  }

  it should "return None for invalid instant" in {
    val xml = <root><updateDate>2024-01-15</updateDate></root>
    XmlParsingHelpers.instantOpt(xml, "updateDate") shouldBe None
  }

}
