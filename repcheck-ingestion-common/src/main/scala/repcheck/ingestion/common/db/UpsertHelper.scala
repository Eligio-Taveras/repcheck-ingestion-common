package repcheck.ingestion.common.db

import doobie.Fragment

object UpsertHelper {

  def upsertFragment(
    table: String,
    columns: List[String],
    conflictColumns: List[String],
    updateColumns: List[String],
  ): Fragment = {
    val quotedTable           = quoteIdentifier(table)
    val quotedColumns         = columns.map(quoteIdentifier)
    val quotedConflictColumns = conflictColumns.map(quoteIdentifier)
    val quotedUpdateColumns   = updateColumns.map(quoteIdentifier)

    val columnList   = quotedColumns.mkString(", ")
    val placeholders = columns.map(_ => "?").mkString(", ")
    val conflictList = quotedConflictColumns.mkString(", ")
    val updateSet = quotedUpdateColumns
      .map(col => s"$col = EXCLUDED.$col")
      .mkString(", ")

    Fragment.const(
      s"""INSERT INTO $quotedTable ($columnList) VALUES ($placeholders) ON CONFLICT ($conflictList) DO UPDATE SET $updateSet"""
    )
  }

  private def quoteIdentifier(name: String): String = {
    val escaped = name.replace("\"", "\"\"")
    s""""$escaped""""
  }

}
