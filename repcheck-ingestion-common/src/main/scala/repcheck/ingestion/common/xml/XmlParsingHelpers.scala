package repcheck.ingestion.common.xml

import java.time.{Instant, LocalDate}
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

  def longOpt(node: Node, tag: String): Option[Long] =
    textOpt(node, tag).flatMap(s => scala.util.Try(s.toLong).toOption)

  def long(node: Node, tag: String): Either[XmlFieldMissing, Long] =
    textOpt(node, tag) match {
      case None => Left(XmlFieldMissing(node.label, tag))
      case Some(s) =>
        scala.util.Try(s.toLong).toOption match {
          case Some(value) => Right(value)
          case None        => Left(XmlFieldMissing(node.label, tag))
        }
    }

  def boolOpt(node: Node, tag: String): Option[Boolean] =
    textOpt(node, tag).flatMap {
      case "true"  => Some(true)
      case "false" => Some(false)
      case _       => None
    }

  def bool(node: Node, tag: String): Either[XmlFieldMissing, Boolean] =
    textOpt(node, tag) match {
      case Some("true")  => Right(true)
      case Some("false") => Right(false)
      case _             => Left(XmlFieldMissing(node.label, tag))
    }

  def instantOpt(node: Node, tag: String): Option[Instant] =
    textOpt(node, tag).flatMap(s => scala.util.Try(Instant.parse(s)).toOption)

  def instant(node: Node, tag: String): Either[XmlFieldMissing, Instant] =
    textOpt(node, tag) match {
      case None => Left(XmlFieldMissing(node.label, tag))
      case Some(s) =>
        scala.util.Try(Instant.parse(s)).toOption match {
          case Some(value) => Right(value)
          case None        => Left(XmlFieldMissing(node.label, tag))
        }
    }

  def children(node: Node, tag: String): Seq[Node] =
    (node \ tag).theSeq

}
