package repcheck.ingestion.common.events

import pureconfig.ConfigSource

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class EventPublisherConfigSpec extends AnyFlatSpec with Matchers {

  "EventPublisherConfig" should "load topicName and source from HOCON" in {
    val hocon = ConfigFactory.parseString(
      """
        |topic-name = "projects/my-project/topics/ingestion-events"
        |source = "bills-pipeline"
        |""".stripMargin
    )

    val result = ConfigSource.fromConfig(hocon).load[EventPublisherConfig]
    result shouldBe Right(
      EventPublisherConfig(
        topicName = "projects/my-project/topics/ingestion-events",
        source = "bills-pipeline",
      )
    )
  }

  it should "fail when topicName is missing" in {
    val hocon = ConfigFactory.parseString(
      """
        |source = "bills-pipeline"
        |""".stripMargin
    )

    val result = ConfigSource.fromConfig(hocon).load[EventPublisherConfig]
    result.isLeft shouldBe true
  }

  it should "fail when source is missing" in {
    val hocon = ConfigFactory.parseString(
      """
        |topic-name = "projects/my-project/topics/ingestion-events"
        |""".stripMargin
    )

    val result = ConfigSource.fromConfig(hocon).load[EventPublisherConfig]
    result.isLeft shouldBe true
  }

}
