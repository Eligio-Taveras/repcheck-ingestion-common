package repcheck.ingestion.common.placeholders

import cats.effect.kernel.MonadCancelThrow
import cats.syntax.all._

import doobie.implicits._
import doobie.util.Write
import doobie.util.transactor.Transactor
import doobie.util.update.Update

/**
 * Generic Doobie-backed [[EntityRepository]] that performs an idempotent insert via Postgres' `ON CONFLICT DO NOTHING`
 * clause.
 *
 * The repository is parameterized by the entity type `T`, which must have a Doobie [[Write]] instance available so that
 * its fields can be bound as prepared statement values. Doobie's auto-derived `Write[T]` covers ordinary case classes;
 * importing `doobie.postgres.implicits._` at the call site brings in `Write` instances for Postgres-specific types
 * (`UUID`, `Instant`, etc.) when needed.
 *
 * The caller supplies the full `INSERT` statement (table name, column list, value placeholders, and `ON CONFLICT`
 * clause). This keeps the repository decoupled from any per-entity SQL while still allowing each entity type to declare
 * its own conflict target — e.g., `ON CONFLICT (natural_key) DO NOTHING` for a unique secondary key, or `ON CONFLICT
 * (id) DO NOTHING` for the primary key. The number and order of `?` placeholders in the SQL must match the order of
 * fields in `T` as Doobie's `Write[T]` derives them.
 *
 * ==SQL injection contract==
 * The `insertSql` argument is treated as a fixed template string supplied by the application at construction time. It
 * never contains user input — all entity field values are bound as parameterized prepared statement values via the
 * `Write[T]` typeclass. This is the same guarantee Doobie's `Update[T]` provides for any literal SQL.
 *
 * @param xa
 *   the Doobie transactor used to execute the insert
 * @param insertSql
 *   the full INSERT statement, including the `ON CONFLICT` clause and `?` placeholders for each field of `T`
 */
class DoobieEntityRepository[F[_]: MonadCancelThrow, T: Write](
  xa: Transactor[F],
  insertSql: String,
) extends EntityRepository[F, T] {

  override def insertIfNotExists(entity: T): F[Unit] =
    Update[T](insertSql).run(entity).transact(xa).void

}
