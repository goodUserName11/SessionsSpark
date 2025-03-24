package parsers

import common.Problem
import events.{DocOpenEvent, QsEvent, CardSearchEvent, SearchEvent, SessionEvent}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.sql.Timestamp

class SessionEventParserSpec extends AnyFlatSpec with Matchers {
  "SessionEventParser" should "correctly parse a valid input string" in {
    val inputString =
      """SESSION_START 13.06.2020_00:53:05
        |CARD_SEARCH_START 13.06.2020_00:53:05
        |$134 региональный
        |CARD_SEARCH_END
        |26175282 PKBO_21787
        |DOC_OPEN 13.06.2020_00:53:05 26175282 PKBO_21787
        |QS 13.06.2020_00:53:05 {удельные}
        |26175282 PKBO_21787
        |DOC_OPEN 13.06.2020_00:53:05 26175282 PKBO_21787
        |SESSION_END 13.06.2020_00:53:05""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val fileName = "/file.txt"
    val sessionParser = new SessionEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val searchId = "26175282"
    val docsFound = List("PKBO_21787")
    val docs = List(DocOpenEvent(timestamp, searchId, "PKBO_21787", List(), "DOC_OPEN"))
    val query = "удельные"
    val queryParams = Map("$134"->"региональный")
    val problems = List()
    val cardSearch = CardSearchEvent(timestamp, docsFound, docs, queryParams, searchId, problems, "CARD_SEARCH")
    val qs = QsEvent(timestamp, docsFound, docs, query, searchId, problems, "QS")
    val events = List[SearchEvent](cardSearch, qs)
    val expectedObject = SessionEvent(timestamp, timestamp, fileName, events, problems, "SESSION")

    val resultDoc = sessionParser.parse(inputString, datetimePattern, fileName)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing content gracefully" in {
    val inputString = ""
      """SESSION_START 13.06.2020_00:53:05
        |SESSION_END 13.06.2020_00:53:05""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val fileName = "/file.txt"
    val sessionParser = new SessionEventParser()

    val timestamp = null
    val problems = List(Problem(1, "SESSION is empty", ""))
    val events = List[SearchEvent]()
    val expectedObject = SessionEvent(timestamp, timestamp, fileName, events, problems, "SESSION")

    val resultDoc = sessionParser.parse(inputString, datetimePattern, fileName)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing field gracefully" in {
    val inputString =
      """SESSION_START
        |SESSION_END 13.06.2020_00:53:05""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val fileName = "/file.txt"
    val sessionParser = new SessionEventParser()

    val timestamp = null
    val endTimestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val problems = List(Problem(1, "Not enough data to parse in SESSION", "SESSION_START"))
    val events = List[SearchEvent]()
    val expectedObject = SessionEvent(timestamp, endTimestamp, fileName, events, problems, "SESSION")

    val resultDoc = sessionParser.parse(inputString, datetimePattern, fileName)

    resultDoc shouldEqual expectedObject
  }

  it should "handle incorrect date gracefully" in {
    val inputString =
      """SESSION_START 13.06.2020
        |SESSION_END 13.06.2020_00:53:05""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val fileName = "/file.txt"
    val sessionParser = new SessionEventParser()

    val timestamp = null
    val endTimestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val problems = List(Problem(1, "Cannot parse date in SESSION_START: Unparseable date: \"13.06.2020\"", "SESSION_START 13.06.2020"))
    val events = List[SearchEvent]()
    val expectedObject = SessionEvent(timestamp, endTimestamp, fileName, events, problems, "SESSION")

    val resultDoc = sessionParser.parse(inputString, datetimePattern, fileName)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing SESSION_END gracefully" in {
    val inputString =
      """SESSION_START 13.06.2020_00:53:05""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val fileName = "/file.txt"
    val sessionParser = new SessionEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val endTimestamp = null
    val problems = List(Problem(1, "Should be SESSION_END in the end of session", "SESSION_START 13.06.2020_00:53:05"))
    val events = List[SearchEvent]()
    val expectedObject = SessionEvent(timestamp, endTimestamp, fileName, events, problems, "SESSION")

    val resultDoc = sessionParser.parse(inputString, datetimePattern, fileName)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing field in SESSION_END gracefully" in {
    val inputString =
      """SESSION_START 13.06.2020_00:53:05
        |SESSION_END""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val fileName = "/file.txt"
    val sessionParser = new SessionEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val endTimestamp = null
    val problems = List(Problem(2, "Not enough data to parse in SESSION_END", "SESSION_END"))
    val events = List[SearchEvent]()
    val expectedObject = SessionEvent(timestamp, endTimestamp, fileName, events, problems, "SESSION")

    val resultDoc = sessionParser.parse(inputString, datetimePattern, fileName)

    resultDoc shouldEqual expectedObject
  }

  it should "handle incorrect date in SESSION_END gracefully" in {
    val inputString =
      """SESSION_START 13.06.2020_00:53:05
        |SESSION_END 13.06.2020""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val fileName = "/file.txt"
    val sessionParser = new SessionEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val endTimestamp = null
    val problems = List(Problem(2, "Cannot parse date in SESSION_END: Unparseable date: \"13.06.2020\"", "SESSION_END 13.06.2020"))
    val events = List[SearchEvent]()
    val expectedObject = SessionEvent(timestamp, endTimestamp, fileName, events, problems, "SESSION")

    val resultDoc = sessionParser.parse(inputString, datetimePattern, fileName)

    resultDoc shouldEqual expectedObject
  }
}
