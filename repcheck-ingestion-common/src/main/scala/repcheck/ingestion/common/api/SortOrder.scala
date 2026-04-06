package repcheck.ingestion.common.api

import pureconfig.ConfigReader

enum SortOrder(val queryValue: String) {
  case UpdateDateAsc  extends SortOrder("updateDate+asc")
  case UpdateDateDesc extends SortOrder("updateDate+desc")
}

object SortOrder {

  given ConfigReader[SortOrder] = ConfigReader[String].map { value =>
    value match {
      case "updateDate+asc"  => SortOrder.UpdateDateAsc
      case "updateDate+desc" => SortOrder.UpdateDateDesc
      case _                 => SortOrder.UpdateDateDesc
    }
  }

}
