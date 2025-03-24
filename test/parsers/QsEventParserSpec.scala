package parsers

import common.Problem
import events.{DocOpenEvent, QsEvent}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.sql.Timestamp

class QsEventParserSpec extends AnyFlatSpec with Matchers{
  "QsEventParser" should "correctly parse a valid input string" in {
    val inputString =
      """QS 13.06.2020_00:53:05 {удельные}
        |26175282 PKBO_21787
        |DOC_OPEN 13.06.2020_00:53:05 26175282 PKBO_21787""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val qsParser = new QsEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val docsFound = List("PKBO_21787")
    val docs = List(DocOpenEvent(timestamp, "26175282", "PKBO_21787", List(), "DOC_OPEN"))
    val query = "удельные"
    val problems = List()
    val expectedObject = QsEvent(timestamp, docsFound, docs, query, "26175282", problems, "QS")

    val resultDoc = qsParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing content gracefully" in {
    val inputString = ""
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val qsParser = new QsEventParser()

    val timestamp = null
    val docsFound = List()
    val docs = List()
    val query = null
    val problems = List(Problem(1, "QS is empty", ""))
    val expectedObject = QsEvent(timestamp, docsFound, docs, query, null, problems, "QS")

    val resultDoc = qsParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing field gracefully" in {
    val inputString =
      """QS {удельные}
        |26175282 PKBO_21787""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val qsParser = new QsEventParser()

    val timestamp = null
    val docsFound = List("PKBO_21787")
    val docs = List()
    val query = "удельные"
    val problems = List(Problem(1, "Not enough data to parse in QS", "QS {удельные}"))
    val expectedObject = QsEvent(timestamp, docsFound, docs, query, "26175282", problems, "QS")

    val resultDoc = qsParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing query gracefully" in {
    val inputString =
      """QS 13.06.2020_00:53:05
        |26175282 PKBO_21787""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val qsParser = new QsEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val docsFound = List("PKBO_21787")
    val docs = List()
    val query = null
    val problems = List(Problem(1, "Query is not present in QS", "QS 13.06.2020_00:53:05"))
    val expectedObject = QsEvent(timestamp, docsFound, docs, query, "26175282", problems, "QS")

    val resultDoc = qsParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle incorrect date gracefully" in {
    val inputString =
      """QS 13.06.2020 {удельные}
        |26175282 PKBO_21787""".stripMargin
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val qsParser = new QsEventParser()

    val timestamp = null
    val docsFound = List("PKBO_21787")
    val docs = List()
    val query = "удельные"
    val problems = List(Problem(1, "Cannot parse date in QS: Unparseable date: \"13.06.2020\"", "QS 13.06.2020 {удельные}"))
    val expectedObject = QsEvent(timestamp, docsFound, docs, query, "26175282", problems, "QS")

    val resultDoc = qsParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing docs found gracefully"
}
