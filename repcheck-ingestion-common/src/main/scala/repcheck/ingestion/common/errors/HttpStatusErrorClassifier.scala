package repcheck.ingestion.common.errors

import repcheck.pipeline.models.errors.{ErrorClass, ErrorClassifier}

/**
 * Generic [[ErrorClassifier]] for HTTP API failures that expose a status code via the [[HttpStatusError]] trait.
 * Subclasses supply only the transient HTTP status codes that should trigger a retry; the `classify` implementation
 * lives here.
 *
 * Errors carrying a status code in `transientStatusCodes` are classified as `ErrorClass.Transient`; everything else —
 * including errors that don't implement `HttpStatusError` — is `ErrorClass.Systemic`.
 *
 * The type parameter `E` is documentary: each per-API classifier states which specific exception it handles (e.g.
 * `HttpStatusErrorClassifier[BillsApiHttpError]`), even though at runtime the pattern match is on the `HttpStatusError`
 * bound. In practice each classifier is plumbed into its own API's retry wrapper, so only instances of `E` reach it.
 * This design avoids the `ClassTag` context bound that strict runtime matching on `E` would require.
 *
 * @param transientStatusCodes
 *   HTTP status codes that indicate a retry-eligible transient failure.
 */
abstract class HttpStatusErrorClassifier[E <: HttpStatusError](transientStatusCodes: Set[Int]) extends ErrorClassifier {

  override def classify(error: Throwable): ErrorClass =
    error match {
      case e: HttpStatusError if transientStatusCodes.contains(e.statusCode) => ErrorClass.Transient
      case _                                                                 => ErrorClass.Systemic
    }

}
