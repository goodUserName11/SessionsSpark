package parsers

import common.Problem
import events.{DocOpenEvent, CardSearchEvent}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.sql.Timestamp

class CardSearchEventParserSpec extends AnyFlatSpec with Matchers{
  "CardSearchEventParser" should "correctly parse a valid input string" in {
    val inputString =
      """CARD_SEARCH_START 13.06.2020_00:53:05
        |$134 региональный
        |CARD_SEARCH_END
        |26175282 PKBO_21787
        |DOC_OPEN 13.06.2020_00:53:05 26175282 PKBO_21787""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val cardParser = new CardSearchEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val docsFound = List("PKBO_21787")
    val docs = List(DocOpenEvent(timestamp, "26175282", "PKBO_21787", List(), "DOC_OPEN"))
    val queryParams = Map("$134"->"региональный")
    val problems = List()
    val expectedObject = CardSearchEvent(timestamp, docsFound, docs, queryParams, "26175282", problems, "CARD_SEARCH")

    val resultDoc = cardParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing content gracefully" in {
    val inputString = ""
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val cardParser = new CardSearchEventParser()

    val timestamp = null
    val docsFound = List()
    val docs = List()
    val queryParams = Map[String, String]()
    val problems = List(Problem(1, "CARD_SEARCH is empty", ""))
    val expectedObject = CardSearchEvent(timestamp, docsFound, docs, queryParams, null, problems, "CARD_SEARCH")

    val resultDoc = cardParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing field gracefully" in {
    val inputString =
      """CARD_SEARCH_START
        |$134 региональный
        |CARD_SEARCH_END
        |26175282 PKBO_21787""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val cardParser = new CardSearchEventParser()

    val timestamp = null
    val docsFound = List("PKBO_21787")
    val docs = List()
    val queryParams = Map("$134" -> "региональный")
    val problems = List(Problem(1, "Not enough data to parse in CARD_SEARCH", "CARD_SEARCH_START"))
    val expectedObject = CardSearchEvent(timestamp, docsFound, docs, queryParams, "26175282", problems, "CARD_SEARCH")

    val resultDoc = cardParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle incorrect date gracefully" in {
    val inputString =
      """CARD_SEARCH_START 13.06.2020
        |$134 региональный
        |CARD_SEARCH_END
        |26175282 PKBO_21787""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val cardParser = new CardSearchEventParser()

    val timestamp = null
    val docsFound = List("PKBO_21787")
    val docs = List()
    val queryParams = Map("$134" -> "региональный")
    val problems = List(Problem(1, "Cannot parse date in CARD_SEARCH: Unparseable date: \"13.06.2020\"", "CARD_SEARCH_START 13.06.2020"))
    val expectedObject = CardSearchEvent(timestamp, docsFound, docs, queryParams, "26175282", problems, "CARD_SEARCH")

    val resultDoc = cardParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing query parameter gracefully" in {
    val inputString =
      """CARD_SEARCH_START 13.06.2020_00:53:05
        |$134 региональный
        |
        |CARD_SEARCH_END
        |26175282 PKBO_21787""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val cardParser = new CardSearchEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val docsFound = List("PKBO_21787")
    val docs = List()
    val queryParams = Map("$134"->"региональный")
    val problems = List(Problem(3, "Query parameter is empty in CARD_SEARCH", ""))
    val expectedObject = CardSearchEvent(timestamp, docsFound, docs, queryParams, "26175282", problems, "CARD_SEARCH")

    val resultDoc = cardParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing query parameter value gracefully" in {
    val inputString =
      """CARD_SEARCH_START 13.06.2020_00:53:05
        |$134
        |CARD_SEARCH_END
        |26175282 PKBO_21787""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val cardParser = new CardSearchEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val docsFound = List("PKBO_21787")
    val docs = List()
    val queryParams = Map("$134" -> null)
    val problems = List(Problem(2, "Query parameter is null in CARD_SEARCH", "$134"))
    val expectedObject = CardSearchEvent(timestamp, docsFound, docs, queryParams, "26175282", problems, "CARD_SEARCH")

    val resultDoc = cardParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing all query parameters gracefully" in {
    val inputString =
      """CARD_SEARCH_START 13.06.2020_00:53:05
        |CARD_SEARCH_END
        |26175282 PKBO_21787""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val cardParser = new CardSearchEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val docsFound = List("PKBO_21787")
    val docs = List()
    val queryParams = Map[String, String]()
    val problems = List(Problem(2, "Query parameters are empty in CARD_SEARCH", "CARD_SEARCH_END"))
    val expectedObject = CardSearchEvent(timestamp, docsFound, docs, queryParams, "26175282", problems, "CARD_SEARCH")

    val resultDoc = cardParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing CARD_SEARCH_END gracefully" in {
    val inputString =
      """CARD_SEARCH_START 13.06.2020_00:53:05
        |$134 региональный
        |26175282 PKBO_21787""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val cardParser = new CardSearchEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val docsFound = List("PKBO_21787")
    val docs = List()
    val queryParams = Map("26175282" -> "PKBO_21787", "$134" -> "региональный")
    val problems = List(Problem(3, "Should be CARD_SEARCH_END in CARD_SEARCH", "26175282 PKBO_21787"))
    val expectedObject = CardSearchEvent(timestamp, docsFound, docs, queryParams, "26175282", problems, "CARD_SEARCH")

    val resultDoc = cardParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }
}
