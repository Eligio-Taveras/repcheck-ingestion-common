package repcheck.ingestion.common.errors

import scala.annotation.tailrec

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

object HttpStatusErrorClassifier {

  /**
   * Wraps an existing [[HttpStatusErrorClassifier]] so that network-level transients (connection drops, socket
   * timeouts, ember stream failures, IO errors) are reclassified as `ErrorClass.Transient` before deferring to the
   * underlying status-code classifier.
   *
   * Walks the cause chain depth-bounded at 16 to defeat both pathological cycles and runaway nesting. Stops at the
   * first [[HttpStatusError]] (boundary case): once we reach a status-bearing exception, classification belongs to the
   * underlying classifier.
   *
   * Use this to consolidate the cause-chain walk that per-API classifiers would otherwise duplicate. Typical wiring:
   * {{{
   * val classifier: ErrorClassifier =
   *   HttpStatusErrorClassifier.transientNetworkAware(MyApiHttpStatusErrorClassifier)
   * }}}
   */
  def transientNetworkAware[E <: HttpStatusError](base: HttpStatusErrorClassifier[E]): ErrorClassifier =
    new ErrorClassifier {
      override def classify(error: Throwable): ErrorClass =
        if (isTransientNetworkError(error)) ErrorClass.Transient
        else base.classify(error)
    }

  /**
   * Walk the cause chain looking for a network-level transient exception. Recurses up to a depth-limit so a
   * pathological cause cycle can't infinite-loop the classifier.
   */
  @tailrec
  private def isTransientNetworkError(t: Throwable, depth: Int = 0): Boolean =
    if (t == null || depth > 16) false
    else
      t match {
        // SocketTimeoutException / ConnectException both extend IOException, so the IOException
        // case below catches them too — listing them separately would fire an "Unreachable case"
        // compiler warning.
        case _: java.io.IOException                   => true
        case _: org.http4s.ember.core.EmberException  => true
        case _: java.util.concurrent.TimeoutException => true
        case _: HttpStatusError                       => false
        case other if other.getCause != null && other.getCause != other =>
          isTransientNetworkError(other.getCause, depth + 1)
        case _ => false
      }

}
