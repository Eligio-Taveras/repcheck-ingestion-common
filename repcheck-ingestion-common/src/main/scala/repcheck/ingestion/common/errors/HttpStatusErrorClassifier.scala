package repcheck.ingestion.common.errors

import repcheck.pipeline.models.errors.{ErrorClass, ErrorClassifier}

/**
 * Generic [[ErrorClassifier]] for HTTP API failures that expose a status code. Subclasses provide (a) the set of
 * transient HTTP status codes that should trigger a retry and (b) how to extract the status code from a given
 * `Throwable`.
 *
 * Errors carrying a status code in `transientStatusCodes` are classified as `ErrorClass.Transient`; everything else —
 * including errors that don't carry a status code at all — is `ErrorClass.Systemic`.
 *
 * The pattern is useful for per-API classifier objects that share identical "transient on selected 4xx/5xx, systemic
 * otherwise" semantics but match on an API-specific Throwable subclass. See [[CongressGovErrorClassifier]] for the
 * canonical application at the ingestion-common layer, and the per-pipeline `*ApiErrorClassifier` objects in
 * data-ingestion for locally-declared per-API variants.
 *
 * @param transientStatusCodes
 *   HTTP status codes that indicate a retry-eligible transient failure.
 */
abstract class HttpStatusErrorClassifier(transientStatusCodes: Set[Int]) extends ErrorClassifier {

  /** Extract the HTTP status code from the Throwable if it carries one; otherwise `None`. */
  protected def extractStatusCode(error: Throwable): Option[Int]

  override def classify(error: Throwable): ErrorClass =
    extractStatusCode(error) match {
      case Some(code) if transientStatusCodes.contains(code) => ErrorClass.Transient
      case _                                                 => ErrorClass.Systemic
    }

}
