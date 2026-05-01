package repcheck.ingestion.common.api

import pureconfig.ConfigReader

/**
 * The Congress.gov bill listing endpoint expects its `sort` query parameter as `"updateDate asc"` / `"updateDate desc"`
 * — i.e., a space between the field name and the direction. Any other separator silently falls back to a
 * non-`updateDate` order (apparently insertion order; bills are returned roughly oldest-inserted first).
 *
 * The `queryValue`s here are the *decoded* form. http4s URL-encodes the space when building the request URL — either as
 * `%20` or as `+` (form-encoding) depending on the encoder, both of which the Congress.gov server decodes back to a
 * literal space, so either is fine.
 *
 * Earlier versions of this enum used `"updateDate+desc"` (literal `+` character in the value). That looked sensible
 * because Congress.gov's documentation shows query params with `+` in URL-form, but it was a category error: the doc's
 * `+` is the URL-encoded space, not a literal `+`. http4s then URL-encoded our literal `+` to `%2B`, which the server
 * decoded back to `+`, which is *not* the separator the API recognizes. Result: every paginated sweep ran in insertion
 * order, not updateDate order.
 */
enum SortOrder(val queryValue: String) {
  case UpdateDateAsc  extends SortOrder("updateDate asc")
  case UpdateDateDesc extends SortOrder("updateDate desc")
}

object SortOrder {

  // Accept both the new "updateDate asc"/"updateDate desc" form and the legacy
  // "updateDate+asc"/"updateDate+desc" form. The legacy form is wrong as a query value (see
  // class doc) but is preserved here for HOCON-config backwards-compat: any deployment that has
  // `sort = "updateDate+desc"` in its application.conf still parses to the right SortOrder
  // case. The ConfigReader is for input parsing; the queryValue on the parsed enum is what
  // actually goes on the wire and is now correct.
  given ConfigReader[SortOrder] = ConfigReader[String].map { value =>
    value match {
      case "updateDate asc" | "updateDate+asc"   => SortOrder.UpdateDateAsc
      case "updateDate desc" | "updateDate+desc" => SortOrder.UpdateDateDesc
      case _                                     => SortOrder.UpdateDateDesc
    }
  }

}
