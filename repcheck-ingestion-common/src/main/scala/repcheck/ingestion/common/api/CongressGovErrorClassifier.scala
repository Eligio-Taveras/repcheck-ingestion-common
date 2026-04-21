package repcheck.ingestion.common.api

import repcheck.ingestion.common.errors.{HttpStatusError, HttpStatusErrorClassifier}

/**
 * Classifier for [[CongressGovApiException]] HTTP failures. Pure wiring of the shared [[HttpStatusErrorClassifier]]:
 * supplies the Congress.gov transient status set (429/500/502/503/504). The `classify` logic is inherited from the
 * base; `CongressGovApiException` provides `statusCode` via [[HttpStatusError]].
 */
object CongressGovErrorClassifier
    extends HttpStatusErrorClassifier[CongressGovApiException](Set(429, 500, 502, 503, 504))

final case class CongressGovApiException(statusCode: Int, message: String)
    extends Exception(s"Congress.gov API error: HTTP $statusCode - $message")
    with HttpStatusError
