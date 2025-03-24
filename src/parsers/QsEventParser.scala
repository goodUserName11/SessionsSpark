package parsers
import common.Problem
import events.{DocOpenEvent, QsEvent}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.MutableList

//"1,2 3, 4,5 6".split(" |,")
//res24: Array[String] = Array(1, 2, 3, "", 4, 5, 6)
class QsEventParser extends AbstractEventParser{
  override def parse(str: String, datetimePattern: String): QsEvent = {
    val docOpenEventParser = new DocOpenEventParser()

    val parsingProblems = MutableList[Problem]()
    val docsOpenedList = MutableList[DocOpenEvent]()

    // Переменные для парсинга даты
    val format = new SimpleDateFormat(datetimePattern)
    var datetime: Date = null
    var timestamp: Timestamp = null

    // Обработка пустой str
    if(str.trim.isEmpty){
      parsingProblems += Problem(1, "QS is empty", str)
      return QsEvent(null, List(), List(), null, null, parsingProblems.toList)
    }

    // Получение строк события
    val lines = str.split(System.lineSeparator())

    // Разделение на информацию о событии и поисковой запрос
    val querySeparately = lines(0).split('{')
    // Если в нем не хватает элементов
    if (querySeparately.length < 2) {
      parsingProblems += Problem(1, "Query is not present in QS", lines(0))
    }

    var query: String = null
    // Получения запроса, если он есть
    if(querySeparately.isDefinedAt(1)) {
      query = querySeparately(1).replace("}", "")
    }

    // Получение информации о событии
    val searchInfo = querySeparately(0).split(" ")

    // Обработка нехватки параметров в строке
    if(searchInfo.length < 2){
      parsingProblems += Problem(1, "Not enough data to parse in QS", lines(0))
    }

    // Парсинг даты
    try {
      if (searchInfo.isDefinedAt(1)) {
        datetime = format.parse(searchInfo(1))
        timestamp = new Timestamp(datetime.getTime)
      }
    } catch {
      // Если дата не парсится
      case e: java.text.ParseException => {
        parsingProblems += Problem(1, s"Cannot parse date in QS: ${e.getMessage}", lines(0))
      }
    }

    // Строка с идентификатором документа и найденными документами
    var docsFoundAndId = Array[String]()
    // Если она существует
    if(lines.isDefinedAt(1)) {
      docsFoundAndId = lines(1).split(" ")
    }

    var searchId: String = null
    var docsFound = Array[String]()
    // Если в строке с идентификатором не хватает параметров
    if(docsFoundAndId.length < 2) {
      parsingProblems += Problem(2, s"Not enough data to parse in QS docs found", lines(1))
    }

    // Парсинг идентификатора поиска, если он есть
    if(docsFoundAndId.isDefinedAt(0)){
      searchId = docsFoundAndId(0)
    }

    // Парсинг найденных документов, если они есть
    if(docsFoundAndId.isDefinedAt(1)) {
      docsFound = docsFoundAndId.drop(1)
    }

    // Строки с событиями открытия документа
    val linesWithDocs = lines.drop(2)

    var currentLine: Int = 0

    linesWithDocs.foreach{line =>
      // Если строка не пустая
      if(line.trim.nonEmpty){
        // Парсинг события открытия документа
        val doc = docOpenEventParser.parse(line, datetimePattern)
        // Добавление события в список
        docsOpenedList += doc
        // Поднятие всех проблем наверх
        doc.parsingProblems.foreach(problem =>
          parsingProblems += Problem(
            // Номер строки из события + текущая строка (если считать с единицы, то она "-1")
            // + первая строка + строка с идентификатором
            problem.lineNumber + currentLine + 2,
            problem.description,
            problem.line
          )
        )
      }
      // Обработка пустой строки
      else{
        parsingProblems += Problem(currentLine + 1, "QS DOC_OPEN line is empty", line)
      }

      currentLine += 1
    }

    return QsEvent(timestamp, docsFound.toList, docsOpenedList.toList, query, searchId, parsingProblems.toList)
  }
}
