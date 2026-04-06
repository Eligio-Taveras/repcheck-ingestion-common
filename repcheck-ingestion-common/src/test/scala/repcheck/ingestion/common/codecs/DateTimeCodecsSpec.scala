package repcheck.ingestion.common.codecs

import java.time.ZonedDateTime

import io.circe.syntax._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DateTimeCodecsSpec extends AnyFlatSpec with Matchers {

  import DateTimeCodecs._

  "DateTimeCodecs" should "encode a ZonedDateTime to ISO format" in {
    val zdt  = ZonedDateTime.parse("2024-03-15T10:30:00Z")
    val json = zdt.asJson
    json.asString.getOrElse("") should include("2024-03-15T10:30")
  }

  it should "decode a valid ISO ZonedDateTime string" in {
    val jsonStr = "\"2024-03-15T10:30:00Z\""
    val result  = io.circe.parser.decode[ZonedDateTime](jsonStr)
    result.isRight shouldBe true
    result.foreach { zdt =>
      zdt.getYear shouldBe 2024
      zdt.getMonthValue shouldBe 3
      zdt.getDayOfMonth shouldBe 15
    }
  }

  it should "fail to decode an invalid date string" in {
    val jsonStr = "\"not-a-date\""
    val result  = io.circe.parser.decode[ZonedDateTime](jsonStr)
    result.isLeft shouldBe true
  }

  it should "roundtrip encode/decode" in {
    val original = ZonedDateTime.parse("2024-06-15T14:30:00+02:00")
    val json     = original.asJson
    val decoded  = json.as[ZonedDateTime]
    decoded.isRight shouldBe true
    decoded.foreach(zdt => zdt.toInstant shouldBe original.toInstant)
  }

}
