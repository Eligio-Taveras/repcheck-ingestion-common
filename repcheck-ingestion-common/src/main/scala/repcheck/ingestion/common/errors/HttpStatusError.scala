package repcheck.ingestion.common.errors

/**
 * Marker trait for project exceptions that carry an HTTP status code. Mixing this in allows the exception to flow
 * through [[HttpStatusErrorClassifier]] without any per-API extractor — the classifier reads `statusCode` directly off
 * the trait.
 *
 * Implementors remain free to expose whatever additional state they like (message, url, cause, etc.); this trait only
 * requires the status code.
 */
trait HttpStatusError extends Throwable {
  def statusCode: Int
}
