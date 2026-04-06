package repcheck.ingestion.common.errors

final case class InvalidProtocol(message: String) extends Exception(message)
