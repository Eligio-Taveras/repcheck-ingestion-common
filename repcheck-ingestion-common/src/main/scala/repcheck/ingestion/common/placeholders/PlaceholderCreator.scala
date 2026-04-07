package repcheck.ingestion.common.placeholders

import repcheck.shared.models.placeholder.HasPlaceholder

trait PlaceholderCreator[F[_]] {

  def ensureExists[T <: Product](
    naturalKey: String,
    repository: EntityRepository[F, T],
  )(using HasPlaceholder[T]): F[Unit]

}
