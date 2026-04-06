package repcheck.ingestion.common.xml

import java.time.LocalDate
import java.time.format.DateTimeFormatter

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
    result.isLeft shouldBe true
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

}
