package repcheck.ingestion.common.api

import repcheck.ingestion.common.errors.HttpStatusErrorClassifier

/**
 * Classifier for [[CongressGovApiException]] HTTP failures. Treats the standard Congress.gov transient status codes
 * (429, 500, 502, 503, 504) as `ErrorClass.Transient` so the retry wrapper retries them; everything else maps to
 * `ErrorClass.Systemic` to fail fast.
 *
 * Concrete wiring of the shared [[HttpStatusErrorClassifier]] base: supplies the transient set and the
 * `CongressGovApiException` status extractor.
 */
object CongressGovErrorClassifier extends HttpStatusErrorClassifier(Set(429, 500, 502, 503, 504)) {

  override protected def extractStatusCode(error: Throwable): Option[Int] =
    error match {
      case e: CongressGovApiException => Some(e.statusCode)
      case _                          => None
    }

}

final case class CongressGovApiException(statusCode: Int, message: String)
    extends Exception(s"Congress.gov API error: HTTP $statusCode - $message")
