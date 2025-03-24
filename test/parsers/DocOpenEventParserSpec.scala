package parsers

import common.Problem
import events.DocOpenEvent

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.sql.Timestamp

class DocOpenEventParserSpec extends AnyFlatSpec with Matchers{
  "DocOpenEventParser" should "correctly parse a valid input string" in {
    val inputString = "DOC_OPEN 13.06.2020_00:53:05 26175282 PKBO_21787"
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val docParser = new DocOpenEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val expectedObject = DocOpenEvent(timestamp, "26175282", "PKBO_21787", List(), "DOC_OPEN")

    val resultDoc = docParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing content gracefully" in {
    val inputString = ""
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val docParser = new DocOpenEventParser()


    val problems = List(Problem(1, "DOC_OPEN line is empty", inputString))
    val expectedObject = DocOpenEvent(null, null, null, problems, "DOC_OPEN")

    val resultDoc = docParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing field gracefully" in {
    val inputString = "DOC_OPEN 13.06.2020_00:53:05 26175282"
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val docParser = new DocOpenEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val problems = List(Problem(1, "Not enough data to parse in DOC_OPEN", inputString))
    val expectedObject = DocOpenEvent(timestamp, "26175282", null, problems, "DOC_OPEN")

    val resultDoc = docParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing or incorrect date gracefully" in {
    val inputString = "DOC_OPEN  26175282 PKBO_21787"
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val docParser = new DocOpenEventParser()

    val problems = List(Problem(1, "Cannot parse date in DOC_OPEN: Unparseable date: \"\"", inputString))
    val expectedObject = DocOpenEvent(null, "26175282", "PKBO_21787", problems, "DOC_OPEN")

    val resultDoc = docParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }

  it should "handle missing searchId gracefully" in {
    val inputString = "DOC_OPEN 13.06.2020_00:53:05  PKBO_21787"
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val docParser = new DocOpenEventParser()

    val timestamp = Timestamp.valueOf("2020-06-13 00:53:05")
    val problems = List()
    val expectedObject = DocOpenEvent(timestamp, "", "PKBO_21787", problems, "DOC_OPEN")

    val resultDoc = docParser.parse(inputString, datetimePattern)

    resultDoc shouldEqual expectedObject
  }
}
