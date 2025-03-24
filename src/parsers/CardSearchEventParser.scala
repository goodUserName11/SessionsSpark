package parsers
import common.Problem
import events.{DocOpenEvent, CardSearchEvent}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable
import scala.collection.mutable.MutableList
import scala.util.control.Breaks.{break, breakable}

/**
 * Парсер события карточки поиска
 */
class CardSearchEventParser extends AbstractEventParser {
  override def parse(str: String, datetimePattern: String): CardSearchEvent = {
    val docOpenEventParser = new DocOpenEventParser()

    val parsingProblems = MutableList[Problem]()
    val docsOpenedList = MutableList[DocOpenEvent]()
    val queryParams = mutable.Map[String, String]()

    // Переменные для парсинга даты
    val format = new SimpleDateFormat(datetimePattern)
    var datetime: Date = null
    var timestamp: Timestamp = null

    // Обработка пустой str
    if (str.trim.isEmpty) {
      parsingProblems += Problem(1, "CARD_SEARCH is empty", str)
      return CardSearchEvent(null, List(), List(), Map(), null, parsingProblems.toList)
    }

    // Получение строк события
    val lines = str.split(System.lineSeparator())

    // Получение информации о событии
    val searchInfo = lines(0).split(" ")

    // Обработка нехватки параметров в строке
    if(searchInfo.length < 2){
      parsingProblems += Problem(1, "Not enough data to parse in CARD_SEARCH", lines(0))
    }
    else {
      // Парсинг даты
      try {
        datetime = format.parse(searchInfo(1))
        timestamp = new Timestamp(datetime.getTime)
      } catch {
        // Если дата не парсится
        case e: java.text.ParseException => {
          parsingProblems += Problem(1, s"Cannot parse date in CARD_SEARCH: ${e.getMessage}", lines(0))
        }
      }
    }

    // Строки без строки с информацией
    val linesWithoutStart = lines.drop(1)
    // Номер текущей строки
    var currentLineWithQueries: Int = 0
    // Найден ли "CARD_SEARCH_END"
    var searchEndFound = false

    breakable {
      for (line <- linesWithoutStart) {
        // Если в строке найден "CARD_SEARCH_END"
        if (line.startsWith("CARD_SEARCH_END")) {
          searchEndFound = true
          break()
        }
        // Если в строке найден "DOC_OPEN"
        if(line.startsWith("DOC_OPEN")){
//          parsingProblems += Problem(currentLineWithQueries + 2, "Should be CARD_SEARCH_END in CARD_SEARCH", line)

          // Сдвигаемся на -2, чтобы указывать на последний параметр поиска
          // (Переступаем через сам "DOC_OPEN" и строку с найденными документами)
//          currentLineWithQueries -= 2
          break()
        }

        // Обработка пустого параметра
        if (line.trim.isEmpty) {
          parsingProblems += Problem(currentLineWithQueries + 2, "Query parameter is empty in CARD_SEARCH", line)
        }
        else {
          // Переменные для парсинга параметра
          val queryParamsStrs = line.split(" ")
          val param = queryParamsStrs(0)
          var paramValue: String = null

          // Если значение параметра существуют
          if (queryParamsStrs.length > 1) {
            paramValue = queryParamsStrs.drop(1).mkString(" ")
          }

          // Обработка отсутствующего значения параметра
          if(paramValue == null){
            parsingProblems += Problem(currentLineWithQueries + 2, "Query parameter is null in CARD_SEARCH", line)
          }

          // Добавление параметра
          queryParams(param) = paramValue
        }

        currentLineWithQueries += 1
      }
    }

    // Обработка пустого списка параметров поиска
    if(queryParams.isEmpty){
      parsingProblems += Problem(currentLineWithQueries + 2, "Query parameters are empty in CARD_SEARCH", linesWithoutStart(currentLineWithQueries))
    }

    // Обработка отсутствия "CARD_SEARCH_END"
    if(!searchEndFound){
      // Сдвигаемся на -2, чтобы указывать на последний параметр поиска
      // (Переступаем через сам "DOC_OPEN" и строку с найденными документами)
      currentLineWithQueries -= 2
      parsingProblems += Problem(currentLineWithQueries + 3, "Should be CARD_SEARCH_END in CARD_SEARCH", linesWithoutStart(currentLineWithQueries + 1))
    }

    // Номер строки с найденными документами
    var foundDocumentsAtLine = currentLineWithQueries + 1

    // Строка с идентификатором документа и найденными документами
    var docsFoundAndId = Array[String]()
    // Если она существует
    if (linesWithoutStart.isDefinedAt(foundDocumentsAtLine)) {
      docsFoundAndId = linesWithoutStart(foundDocumentsAtLine).split(" ")
    }

    var searchId: String = null
    var docsFound = Array[String]()
    // Если в строке с идентификатором не хватает параметров
    if (docsFoundAndId.length < 2) {
      parsingProblems += Problem(foundDocumentsAtLine + 1, s"Not enough data to parse in CARD_SEARCH docs found", linesWithoutStart(foundDocumentsAtLine))
    }

    // Парсинг идентификатора поиска, если он есть
    if (docsFoundAndId.isDefinedAt(0)) {
      searchId = docsFoundAndId(0)
    }

    // Парсинг найденных документов, если они есть
    if (docsFoundAndId.isDefinedAt(1)) {
      docsFound = docsFoundAndId.drop(1)
    }

    // Строки с событиями открытия документа
    val linesWithDocs = linesWithoutStart.drop(foundDocumentsAtLine + 1)

    var currentLine: Int = 0

    linesWithDocs.foreach { line =>
      // Если строка не пустая
      if (line.trim.nonEmpty) {
        // Парсинг события открытия документа
        val doc = docOpenEventParser.parse(line, datetimePattern)
        // Добавление события в список
        docsOpenedList += doc
        // Поднятие всех проблем наверх
        doc.parsingProblems.foreach(problem =>
          parsingProblems += Problem(
            // Номер строки из события + текущая строка (если считать с единицы, то она "-1")
            // + смещение из-за параметров поиска + первая строка + строка с идентификатором
            problem.lineNumber + currentLine + foundDocumentsAtLine + 2,
            problem.description,
            problem.line
          )
        )
      }
      // Обработка пустой строки
      else {
        parsingProblems += Problem(currentLine + 1, "CARD_SEARCH DOC_OPEN line is empty", line)
      }

      currentLine += 1
    }

    return CardSearchEvent(timestamp, docsFound.toList, docsOpenedList.toList, queryParams.toMap, searchId, parsingProblems.toList)
  }
}
