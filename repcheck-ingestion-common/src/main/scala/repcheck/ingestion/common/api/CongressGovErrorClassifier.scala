package repcheck.ingestion.common.api

import repcheck.pipeline.models.errors.{ErrorClass, ErrorClassifier}

object CongressGovErrorClassifier extends ErrorClassifier {

  private val transientStatusCodes: Set[Int] = Set(429, 500, 502, 503, 504)

  def classify(error: Throwable): ErrorClass =
    extractStatusCode(error) match {
      case Some(code) =>
        if (transientStatusCodes.contains(code)) {
          ErrorClass.Transient
        } else {
          ErrorClass.Systemic
        }
      case None =>
        ErrorClass.Systemic
    }

  private def extractStatusCode(error: Throwable): Option[Int] =
    error match {
      case e: CongressGovApiException => Some(e.statusCode)
      case _                          => None
    }

}

final case class CongressGovApiException(statusCode: Int, message: String)
    extends Exception(s"Congress.gov API error: HTTP $statusCode - $message")
