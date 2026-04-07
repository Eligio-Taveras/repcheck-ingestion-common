package repcheck.ingestion.common.placeholders

trait EntityRepository[F[_], T] {

  def insertIfNotExists(entity: T): F[Unit]
}
