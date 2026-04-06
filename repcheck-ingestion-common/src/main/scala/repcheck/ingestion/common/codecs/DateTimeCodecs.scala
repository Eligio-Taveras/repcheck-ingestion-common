package repcheck.ingestion.common.codecs

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import io.circe.{Decoder, Encoder}

object DateTimeCodecs {

  val zonedDateTimeFormatter: DateTimeFormatter = DateTimeFormatter.ISO_ZONED_DATE_TIME

  implicit val zonedDateTimeEncoder: Encoder[ZonedDateTime] =
    Encoder.encodeString.contramap[ZonedDateTime](_.format(zonedDateTimeFormatter))

  implicit val zonedDateTimeDecoder: Decoder[ZonedDateTime] =
    Decoder.decodeString.emap { str =>
      try
        Right(ZonedDateTime.parse(str, zonedDateTimeFormatter))
      catch {
        case e: java.time.format.DateTimeParseException =>
          Left(s"Failed to parse ZonedDateTime: ${e.getMessage}")
      }
    }

}
