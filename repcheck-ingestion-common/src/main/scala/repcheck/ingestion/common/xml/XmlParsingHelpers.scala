package repcheck.ingestion.common.xml

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.xml.Node

import repcheck.ingestion.common.errors.XmlFieldMissing

object XmlParsingHelpers {

  def textOpt(node: Node, tag: String): Option[String] = {
    val nodes = node \ tag
    if (nodes.isEmpty) {
      None
    } else {
      val content = nodes.text.trim
      if (content.isEmpty) { None }
      else { Some(content) }
    }
  }

  def text(node: Node, tag: String): Either[XmlFieldMissing, String] =
    textOpt(node, tag) match {
      case Some(value) => Right(value)
      case None        => Left(XmlFieldMissing(node.label, tag))
    }

  def intOpt(node: Node, tag: String): Option[Int] =
    textOpt(node, tag).flatMap(s => scala.util.Try(s.toInt).toOption)

  def int(node: Node, tag: String): Either[XmlFieldMissing, Int] =
    textOpt(node, tag) match {
      case None => Left(XmlFieldMissing(node.label, tag))
      case Some(s) =>
        scala.util.Try(s.toInt).toOption match {
          case Some(value) => Right(value)
          case None        => Left(XmlFieldMissing(node.label, tag))
        }
    }

  def dateOpt(
    node: Node,
    tag: String,
    format: DateTimeFormatter,
  ): Option[LocalDate] =
    textOpt(node, tag).flatMap(s => scala.util.Try(LocalDate.parse(s, format)).toOption)

  def children(node: Node, tag: String): Seq[Node] =
    (node \ tag).theSeq

}
