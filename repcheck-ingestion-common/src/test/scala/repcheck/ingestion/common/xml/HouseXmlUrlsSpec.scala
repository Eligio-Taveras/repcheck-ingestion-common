package repcheck.ingestion.common.xml

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HouseXmlUrlsSpec extends AnyFlatSpec with Matchers {

  "memberData" should "return correct URL" in {
    HouseXmlUrls.memberData shouldBe "https://clerk.house.gov/xml/lists/MemberData.xml"
  }

  it should "start with https://" in {
    HouseXmlUrls.memberData should startWith("https://")
  }

}
