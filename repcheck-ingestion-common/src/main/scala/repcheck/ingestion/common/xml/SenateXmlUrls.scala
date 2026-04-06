package repcheck.ingestion.common.xml

object SenateXmlUrls {

  private val baseUrl: String = "https://www.senate.gov"

  def rollCallVote(congress: Int, session: Int, voteNumber: Int): String = {
    val paddedVote = f"$voteNumber%05d"
    s"$baseUrl/legislative/LIS/roll_call_votes/vote${congress.toString}${session.toString}/vote_${congress.toString}_${session.toString}_$paddedVote.xml"
  }

  def rollCallIndex(congress: Int, session: Int): String =
    s"$baseUrl/legislative/LIS/roll_call_votes/vote${congress.toString}${session.toString}/vote_summary.xml"

  val memberContactInfo: String =
    s"$baseUrl/general/contact_information/senators_cfm.xml"

  val committeeMemberData: String =
    s"$baseUrl/legislative/LIS_MEMBER/cvc_member_data.xml"

  val senatorLookup: String =
    s"$baseUrl/about/senator-lookup.xml"

}
