package repcheck.ingestion.common.changes

import java.time.Instant

object ChangeDetector {

  private val skippedFields: Set[String] = Set("createdAt", "updatedAt")

  def detect[T <: Product](
    incoming: T,
    stored: Option[T],
    incomingUpdateDate: Instant,
    storedUpdateDate: Option[Instant],
  ): ChangeReport[T] =
    stored match {
      case None =>
        ChangeReport.New(incoming)
      case Some(existing) =>
        storedUpdateDate match {
          case Some(storedDate) if incomingUpdateDate.isBefore(storedDate) =>
            ChangeReport.Unchanged()
          case Some(storedDate) if incomingUpdateDate.equals(storedDate) =>
            ChangeReport.Unchanged()
          case _ =>
            val diffs = diffProducts(incoming, existing, prefix = "")
            if (diffs.isEmpty) {
              ChangeReport.Unchanged()
            } else {
              ChangeReport.Updated(diffs)
            }
        }
    }

  private[changes] def diffProducts(
    incoming: Product,
    stored: Product,
    prefix: String,
  ): List[FieldDiff] = {
    val incomingNames  = incoming.productElementNames.toList
    val incomingValues = incoming.productIterator.toList
    val storedValues   = stored.productIterator.toList

    incomingNames
      .zip(incomingValues.zip(storedValues))
      .flatMap {
        case (fieldName, (inVal, stoVal)) =>
          if (skippedFields.contains(fieldName)) {
            List.empty[FieldDiff]
          } else {
            val qualifiedName =
              if (prefix.isEmpty) { fieldName }
              else { s"$prefix.$fieldName" }
            diffValues(qualifiedName, inVal, stoVal)
          }
      }
  }

  private def diffValues(
    fieldName: String,
    inVal: Any,
    stoVal: Any,
  ): List[FieldDiff] =
    (inVal, stoVal) match {
      case (inList: List[?], stoList: List[?]) =>
        diffLists(fieldName, inList, stoList)
      case (inProduct: Product, stoProduct: Product) if !isSimpleWrapper(inProduct) && !isSimpleWrapper(stoProduct) =>
        diffProducts(inProduct, stoProduct, prefix = fieldName)
      case _ =>
        if (inVal == stoVal) {
          List.empty[FieldDiff]
        } else {
          List(FieldDiff(fieldName, stoVal, inVal))
        }
    }

  private def isSimpleWrapper(p: Product): Boolean =
    p match {
      case _: Option[?] => true
      case _: Tuple     => true
      case _            => false
    }

  private def diffLists(
    fieldName: String,
    inList: List[?],
    stoList: List[?],
  ): List[FieldDiff] = {
    val hasNaturalKeys = inList.headOption.orElse(stoList.headOption) match {
      case Some(_: HasNaturalKey) => true
      case _                      => false
    }

    if (hasNaturalKeys) {
      diffKeyedLists(fieldName, inList, stoList)
    } else {
      diffUnkeyedLists(fieldName, inList, stoList)
    }
  }

  private def diffKeyedLists(
    fieldName: String,
    inList: List[?],
    stoList: List[?],
  ): List[FieldDiff] = {
    val inMap: Map[String, Any] = inList.collect {
      case h: HasNaturalKey =>
        h.naturalKey -> h
    }.toMap

    val stoMap: Map[String, Any] = stoList.collect {
      case h: HasNaturalKey =>
        h.naturalKey -> h
    }.toMap

    val additions: List[FieldDiff] = inMap.keys.toList.sorted
      .filterNot(stoMap.contains)
      .map(key => FieldDiff(s"$fieldName[+$key]", (), inMap(key)))

    val removals: List[FieldDiff] = stoMap.keys.toList.sorted
      .filterNot(inMap.contains)
      .map(key => FieldDiff(s"$fieldName[-$key]", stoMap(key), ()))

    val modifications: List[FieldDiff] = inMap.keys.toList.sorted
      .filter(stoMap.contains)
      .flatMap { key =>
        (inMap(key), stoMap(key)) match {
          case (inProduct: Product, stoProduct: Product) =>
            diffProducts(inProduct, stoProduct, prefix = s"$fieldName[$key]")
          case (inVal, stoVal) if inVal != stoVal =>
            List(FieldDiff(s"$fieldName[$key]", stoVal, inVal))
          case _ =>
            List.empty[FieldDiff]
        }
      }

    additions ++ removals ++ modifications
  }

  private def diffUnkeyedLists(
    fieldName: String,
    inList: List[?],
    stoList: List[?],
  ): List[FieldDiff] = {
    val inSorted  = inList.map(_.toString).sorted
    val stoSorted = stoList.map(_.toString).sorted
    if (inSorted == stoSorted) {
      List.empty[FieldDiff]
    } else {
      List(FieldDiff(fieldName, stoList, inList))
    }
  }

}
