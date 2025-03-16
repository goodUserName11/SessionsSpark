import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable

object Main {
  def main(args: Array[String]): Unit = {
    val sessionsFile = "/Сессии/"
    val searchingId = "ACC_45616"
    val countOutput = "/opt/spark/work-dir/consres/count.txt"
    val countByDateOutput = "/opt/spark/work-dir/consres/countByDate.txt"

    countSearches(sessionsFile, countOutput, searchingId)
    countQuickByDate(sessionsFile, countByDateOutput)
  }

  /**
   * Подсчет количества раз, когда в карточке производили поиск документа с указанным идентификатором
   * @param sessionsFile Файл или директория с входными данными
   * @param outputFile Директория с выходными данными
   * @param searchingId Идентификатор для подсчета
   */
  def countSearches(sessionsFile: String, outputFile: String, searchingId: String): Unit ={
    // Создание SparkSession
    val spark = SparkSession.builder
      .appName("Count card searches")
      .getOrCreate()
    import spark.implicits._
    val sessionsData = spark.read
      .option("encoding", "windows-1251")
      .text(sessionsFile)
      .as[String]

    // Предыдущая строка
    var previousLine: Option[String] = None
    // Найденные документы при поиске
    var searchResults: Seq[String] = Seq()

    // Перебор строк
    sessionsData.collect().foreach { line =>
      // Если предущая строка конец карточки поиска, записываем ее. На следующей строке - найденные документы
      if (line.startsWith("CARD_SEARCH_END")) {
        previousLine = Some(line)
      } else if (previousLine.isDefined) {
        searchResults = searchResults :+ line
        previousLine = None
      }
    }

    // Количество найденных документов с идентификатором
    val searchesCount = searchResults.count(_.contains(searchingId))

    // Создание RDD
    val resultRDD = spark.sparkContext.parallelize(Seq(s"Count_$searchingId: $searchesCount"))
    // Сохранения результата в файл
    resultRDD.coalesce(1).saveAsTextFile(outputFile)

    spark.stop()
  }

  /**
   *   Хранение информации о событии
   */
  case class SessionEvent(date: String, eventType: String, details: String)

  /**
   * Подсчет количества открытий каждого документа, найденного через быстрый поиск за каждый день
   * @param sessionsFile Файл или директория с входными данными
   * @param outputFile Директория с выходными данными
   */
  def countQuickByDate(sessionsFile: String, outputFile: String): Unit ={
    // Создание SparkSession
    val spark = SparkSession.builder
      .appName("Count QS docs by date")
      .getOrCreate()
    // Чтобы избежать No implicit found for parameter Encoder[...]
    import spark.implicits._
    val sessionsData = spark.read.option("encoding", "windows-1251").text(sessionsFile).as[String]

    // Хранение информации о событии
//    case class SessionEvent(date: String, eventType: String, details: String)

    // Отфильтровываем ненужные строки
    val sessionsDataFiltered = sessionsData.filter(line=>
      line.startsWith("QS")
      || line.startsWith("CARD_SEARCH_END")
      || line.startsWith("DOC_OPEN")
    )

    // Преобразуем данные в Dataset[SessionEvent]
    val events = sessionsDataFiltered.flatMap { line =>
      val parts = line.split(" ", 3) // Разделяем по пробелам
      if (parts.length >= 2) {
        val dateString = parts(1) // Дата и время
        val eventType = parts(0) // Тип события
        val details = if (parts.length > 2) parts(2) else "" // Дополнительные детали события
        Some(SessionEvent(dateString, eventType, details))
      } else {
        None
      }
    }

    // Для подсчета количества открытий документов
    val documentOpenCounts = mutable.Map[String, mutable.Map[String, Int]]()
    // Для учитывания события CARD_SEARCH_END для дальнейшего игнорирования DOC_OPEN
    var lastSearchEvent: SessionEvent = null

    // Перебор событий
    events.collect.foreach{ event =>
      // Запоминаем последнее событие поиска
      if(event.eventType.startsWith("QS")
        || event.eventType.startsWith("CARD_SEARCH_END")){
        lastSearchEvent = event
      }
      if(event.eventType.startsWith("DOC_OPEN")
        && lastSearchEvent.eventType.startsWith("QS")){
        val docId = event.details.split(" ").last
        var openDate = event.date.split("_")(0)
        // Поменялся формат или есть ошибки
        if(openDate.isEmpty) {
          openDate = lastSearchEvent.date.split("_")(0)
        }
        if(!documentOpenCounts.contains(openDate)){
          documentOpenCounts(openDate) = mutable.Map[String, Int]()
        }
        documentOpenCounts(openDate)(docId) = documentOpenCounts(openDate).getOrElse(docId, 0) + 1
      }
    }

    // Подготовка и вывод результата
//    documentOpenCounts.foreach { case (date, docs) =>
//      docs.foreach { case (docId, count) =>
//        println(s"$date $docId $count")
//      }
//    }

    // Преобразование в последовательность строк
    val dataToWrite = documentOpenCounts.flatMap { case (date, docs) =>
      docs.map { case (docId, count) =>
        s"$date $docId $count" // Строка формата: docId,userId,count
      }
    }.toSeq

    // Создание RDD
    val rdd = spark.sparkContext.parallelize(dataToWrite)
    // Запись в текстовый файл (Для удобства в один файл)
    rdd.coalesce(1).saveAsTextFile(outputFile)

    spark.stop()
  }
}