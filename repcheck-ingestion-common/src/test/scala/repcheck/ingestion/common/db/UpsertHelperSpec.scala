package repcheck.ingestion.common.db

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UpsertHelperSpec extends AnyFlatSpec with Matchers {

  "UpsertHelper" should "generate correct INSERT structure" in {
    val fragment = UpsertHelper.upsertFragment(
      table = "members",
      columns = List("id", "name", "email"),
      conflictColumns = List("id"),
      updateColumns = List("name", "email"),
    )

    val sql = fragment.internals.sql
    sql should include("""INSERT INTO "members" ("id", "name", "email")""")
    sql should include("VALUES (?, ?, ?)")
  }

  it should "target correct columns in ON CONFLICT" in {
    val fragment = UpsertHelper.upsertFragment(
      table = "bills",
      columns = List("bill_id", "title", "status"),
      conflictColumns = List("bill_id"),
      updateColumns = List("title", "status"),
    )

    val sql = fragment.internals.sql
    sql should include("""ON CONFLICT ("bill_id")""")
  }

  it should "include only update columns in DO UPDATE SET" in {
    val fragment = UpsertHelper.upsertFragment(
      table = "votes",
      columns = List("vote_id", "member_id", "position", "date"),
      conflictColumns = List("vote_id", "member_id"),
      updateColumns = List("position"),
    )

    val sql = fragment.internals.sql
    sql should include("""DO UPDATE SET "position" = EXCLUDED."position"""")
    sql should not include """"date" = EXCLUDED."date""""
  }

  it should "handle multiple conflict columns" in {
    val fragment = UpsertHelper.upsertFragment(
      table = "vote_positions",
      columns = List("vote_id", "member_id", "position"),
      conflictColumns = List("vote_id", "member_id"),
      updateColumns = List("position"),
    )

    val sql = fragment.internals.sql
    sql should include("""ON CONFLICT ("vote_id", "member_id")""")
  }

  it should "double-quote all identifiers" in {
    val fragment = UpsertHelper.upsertFragment(
      table = "my_table",
      columns = List("col_a", "col_b"),
      conflictColumns = List("col_a"),
      updateColumns = List("col_b"),
    )

    val sql = fragment.internals.sql
    sql should include(""""my_table"""")
    sql should include(""""col_a"""")
    sql should include(""""col_b"""")
  }

  it should "handle special characters in names safely" in {
    val fragment = UpsertHelper.upsertFragment(
      table = """my"table""",
      columns = List("""col"a""", "col_b"),
      conflictColumns = List("""col"a"""),
      updateColumns = List("col_b"),
    )

    val sql = fragment.internals.sql
    // Double quotes inside identifiers should be escaped by doubling them
    sql should include(""""my""table"""")
    sql should include(""""col""a"""")
  }

  it should "handle multiple update columns" in {
    val fragment = UpsertHelper.upsertFragment(
      table = "members",
      columns = List("id", "first_name", "last_name", "party"),
      conflictColumns = List("id"),
      updateColumns = List("first_name", "last_name", "party"),
    )

    val sql = fragment.internals.sql
    sql should include(""""first_name" = EXCLUDED."first_name"""")
    sql should include(""""last_name" = EXCLUDED."last_name"""")
    sql should include(""""party" = EXCLUDED."party"""")
  }

}
