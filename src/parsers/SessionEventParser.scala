package parsers

import common.Problem
import events.{SearchEvent, SessionEvent}

import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.util.Date
import scala.collection.mutable.MutableList

class SessionEventParser extends  AbstractEventParser {
  /**
   * Метод парсинга строки в событие
   *
   * @param str строка для парсинга
   * @param datetimePattern шаблон даты для парсинга
   * @return событие
   */
  def parse(str: String, datetimePattern: String): SessionEvent = {
    val cardSearchParser = new CardSearchEventParser()
    val qsSearchParser = new QsEventParser()

    val parsingProblems = MutableList[Problem]()
    val eventsList = MutableList[SearchEvent]()

    // Переменные для парсинга даты
    val format = new SimpleDateFormat(datetimePattern)
    var datetime: Date = null
    var timestamp: Timestamp = null

    var endDatetime: Date = null
    var endTimestamp: Timestamp = null

    // Обработка пустой str
    if (str.trim.isEmpty) {
      parsingProblems += Problem(1, "SESSION is empty", str)
      return SessionEvent(null, null, List(), parsingProblems.toList)
    }

    // Получение строк события
    val lines = str.trim.split(System.lineSeparator())

    // Получение информации о событии
    val sessionInfo = lines(0).split(" ")

    // Обработка нехватки параметров в строке
    if(sessionInfo.length < 2){
      parsingProblems += Problem(1, "Not enough data to parse in SESSION", lines(0))
    }
    else{
      // Парсинг даты
      try {
        datetime = format.parse(sessionInfo(1))
        timestamp = new Timestamp(datetime.getTime)
      } catch {
        // Если дата не парсится
        case e: java.text.ParseException => {
          parsingProblems += Problem(1, s"Cannot parse date in SESSION_START: ${e.getMessage}", lines(0))
        }
      }
    }

    // Строки с событиями
    var inSession = lines.drop(1)

    // Если присутствует "SESSION_END"
    if(lines.last.startsWith("SESSION_END")){
      inSession = inSession.dropRight(1)

      // // Получение информации о "SESSION_END"
      val endSessionInfo = lines.last.split(" ")

      // Обработка нехватки параметров в строке
      if(endSessionInfo.length < 2){
        parsingProblems += Problem(lines.length, "Not enough data to parse in SESSION_END", lines.last)
      }
      else {
        // Парсинг даты
        try {
          endDatetime = format.parse(endSessionInfo(1))
          endTimestamp = new Timestamp(endDatetime.getTime)
        } catch {
          // Если дата не парсится
          case e: java.text.ParseException => {
            parsingProblems += Problem(lines.length, s"Cannot parse date in SESSION_END: ${e.getMessage}", lines.last)
          }
        }
      }
    }
    // Если отсутствует "SESSION_END"
    else{
      parsingProblems += Problem(lines.length, "Should be SESSION_END in the end of session", lines.last)
    }

    // Название события поиска
    var searchEventName: String = ""
    // Текущая строка
    var currentLine: Int = 0
    // Номер строки, на которой начинается событие поиска
    var searchEventStart: Int = 0
    // Найдены ли начало и конец события поиска
    var isSearchEvent: Boolean = false

    //foreach закончиться раньше, чем мы добавим все строки (проверять что строка последняя, значит поиск закончился)
    inSession.foreach{ line =>
      // Если это начало события поиска
      if(line.startsWith("QS") || line.startsWith("CARD_SEARCH_START")){
        // если начало события не равно текущей строке
        if(searchEventStart != currentLine){
          isSearchEvent = true
        }

        // Получение названия события
        searchEventName = line.split(" ")(0)

        searchEventStart = currentLine
      }
      currentLine += 1

      // Если текущая строка последняя
      if(currentLine == inSession.length){
        isSearchEvent = true
      }

      // Если найден конец события поиска
      if (isSearchEvent){
        isSearchEvent = false

        // Получение строк внутреннего события
        // Берем список строк и убираем из него:
        // первые n строк до searchEventStart
        // последние n строк до строки currentLine (не включая)
        val searchEventStr = inSession
          .take(currentLine)
          .drop(searchEventStart)
          .mkString(System.lineSeparator())

        // Событие поиска
        var searchEvent: SearchEvent = null

        // Парсинг соответствующего события поиска по имени
        searchEventName match {
          case "QS" => searchEvent = qsSearchParser.parse(searchEventStr, datetimePattern)
          case "CARD_SEARCH_START" => searchEvent = cardSearchParser.parse(searchEventStr, datetimePattern)
          case _ => None
        }

        // Если событие получено корректно
        if (searchEvent != null) {
          // Добавление события к списку
          eventsList += searchEvent
          // Поднятие проблем наверх
          searchEvent.parsingProblems.foreach(problem =>
            parsingProblems += Problem(
              // Номер строки из события + текущая строка (если считать с единицы, то она "-1")
              // + первая строка
              problem.lineNumber + searchEventStart + 1,
              problem.description,
              problem.line
            )
          )
        }
      }
    }

    return SessionEvent(timestamp, endTimestamp, eventsList.toList, parsingProblems.toList)
  }
}
