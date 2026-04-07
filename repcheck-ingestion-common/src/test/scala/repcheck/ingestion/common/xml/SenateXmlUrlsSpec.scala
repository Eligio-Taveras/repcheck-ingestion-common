package repcheck.ingestion.common.xml

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SenateXmlUrlsSpec extends AnyFlatSpec with Matchers {

  "rollCallVote" should "produce correct URL for given congress, session, and vote number" in {
    val url = SenateXmlUrls.rollCallVote(118, 1, 42)
    url shouldBe "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1181/vote_118_1_00042.xml"
  }

  it should "zero-pad vote numbers to 5 digits" in {
    val url = SenateXmlUrls.rollCallVote(117, 2, 3)
    url shouldBe "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1172/vote_117_2_00003.xml"
  }

  it should "handle large vote numbers" in {
    val url = SenateXmlUrls.rollCallVote(118, 1, 12345)
    url shouldBe "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1181/vote_118_1_12345.xml"
  }

  "rollCallIndex" should "produce correct index URL" in {
    val url = SenateXmlUrls.rollCallIndex(118, 1)
    url shouldBe "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1181/vote_summary.xml"
  }

  "memberContactInfo" should "return correct URL" in {
    SenateXmlUrls.memberContactInfo shouldBe "https://www.senate.gov/general/contact_information/senators_cfm.xml"
  }

  "committeeMemberData" should "return correct URL" in {
    SenateXmlUrls.committeeMemberData shouldBe "https://www.senate.gov/legislative/LIS_MEMBER/cvc_member_data.xml"
  }

  "senatorLookup" should "return correct URL" in {
    SenateXmlUrls.senatorLookup shouldBe "https://www.senate.gov/about/senator-lookup.xml"
  }

  "all URLs" should "start with https://" in {
    val _ = SenateXmlUrls.rollCallVote(118, 1, 1) should startWith("https://")
    val _ = SenateXmlUrls.rollCallIndex(118, 1) should startWith("https://")
    val _ = SenateXmlUrls.memberContactInfo should startWith("https://")
    val _ = SenateXmlUrls.committeeMemberData should startWith("https://")
    SenateXmlUrls.senatorLookup should startWith("https://")
  }

}
