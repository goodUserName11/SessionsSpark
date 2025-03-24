package parsers
import events.{DocOpenEvent}
import common.Problem

import java.util.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.collection.mutable.MutableList

/**
 * Парсер события открития документа
 */
class DocOpenEventParser extends AbstractEventParser{
  override def parse(str: String, datetimePattern: String): DocOpenEvent = {
    val parsingProblems = MutableList[Problem]()

    // Переменные для парсинга даты
    val format = new SimpleDateFormat(datetimePattern)
    var datetime: Date = null
    var timestamp: Timestamp = null

    // Обработка пустой str
    if(str.trim.isEmpty){
      parsingProblems += Problem(1, "DOC_OPEN line is empty", str)
      return DocOpenEvent(null, null, null, parsingProblems.toList)
    }

    // Получение информации об открытие документа
    val docOpenInfo = str.split(" ")

    // Обработка нехватки параметров в строке
    if(docOpenInfo.length < 4){
      parsingProblems += Problem(1, "Not enough data to parse in DOC_OPEN", str)
    }

    // Парсинг даты
    try {
      if(docOpenInfo.isDefinedAt(1)) {
        datetime = format.parse(docOpenInfo(1))
        timestamp = new Timestamp(datetime.getTime)
      }
    } catch {
      // Если дата не парсится
      case e: java.text.ParseException => {
        parsingProblems += Problem(1, s"Cannot parse date in DOC_OPEN: ${e.getMessage}", str)
      }
    }

    // Парсинг идентификатора поиска, если он есть
    var searchId: String = null
    if(docOpenInfo.isDefinedAt(2)) {
      searchId = docOpenInfo(2)
    }

    // Парсинг идентификатора документа, если он есть
    var docId: String = null
    if(docOpenInfo.isDefinedAt(3)) {
      docId = docOpenInfo(3)

      // Проверка, что идентификатор имеет правильный формат
      val docIdParts = docId.split("_").filter(_.nonEmpty)
      if (docIdParts.length < 2) {
        parsingProblems += Problem(1, "DocId should have pattern '<BASE>_<DOC_NUMBER>'", str)
      }
    }
    return DocOpenEvent(timestamp, searchId, docId, parsingProblems.toList)
  }
}
