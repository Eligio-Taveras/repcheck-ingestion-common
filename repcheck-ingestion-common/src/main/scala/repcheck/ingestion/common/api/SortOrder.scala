package repcheck.ingestion.common.api

import pureconfig.ConfigReader

enum SortOrder(val queryValue: String) {
  case UpdateDateAsc  extends SortOrder("updateDate+asc")
  case UpdateDateDesc extends SortOrder("updateDate+desc")
}

object SortOrder {

  given ConfigReader[SortOrder] = ConfigReader[String].map { value =>
    value.toLowerCase match {
      case "updatedateasc" | "update-date-asc"   => SortOrder.UpdateDateAsc
      case "updatedatedesc" | "update-date-desc" => SortOrder.UpdateDateDesc
      case _                                     => SortOrder.UpdateDateDesc
    }
  }

}
