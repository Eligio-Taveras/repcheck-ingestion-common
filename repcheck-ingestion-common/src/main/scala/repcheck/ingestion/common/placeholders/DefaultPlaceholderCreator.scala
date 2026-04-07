package repcheck.ingestion.common.placeholders

import repcheck.shared.models.placeholder.HasPlaceholder

final class DefaultPlaceholderCreator[F[_]] extends PlaceholderCreator[F] {

  override def ensureExists[T <: Product](
    naturalKey: String,
    repository: EntityRepository[F, T],
  )(using hp: HasPlaceholder[T]): F[Unit] = {
    val entity = hp.placeholder(naturalKey)
    repository.insertIfNotExists(entity)
  }

}
