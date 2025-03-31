package spark

import events.{CardSearchEvent, QsEvent}
import parsers.SessionEventParser
import common.EventsEncoders._
import common.FileSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.date_format

import java.nio.charset.Charset

object SparkTasks {

  /**
   * Подсчет количества раз, когда в карточке производили поиск документа с указанным идентификатором
   *
   * @param sessionsFiles   Файл или директория с входными данными
   * @param datetimePattern Шаблон даты-времени
   * @param outputFile      Директория с выходными данными
   * @param searchingId     Идентификатор для подсчета
   */
  def countSearches(sessionsFiles: String, datetimePattern: String, outputFile: String, searchingId: String): Unit = {
    // Создание spark сессии
    val spark = SparkSession.builder
      .appName("Count card searches")
      .getOrCreate()
    // Подключение приведений данных
    import spark.implicits._
    // Чтение файлов побайтово
    val binaryFilesDF = spark.read
      .format("binaryFile")
      .load(sessionsFiles)

    // Получение из битовых массивов списка объектов сессий
    val sessionEvents = binaryFilesDF.flatMap { file =>
      val filePath = file.getString(0)
      val strContent = new String(
        file.get(3).asInstanceOf[Array[Byte]],
        Charset.forName("windows-1251")
      )
      val sessionParser: SessionEventParser = new SessionEventParser()
      val session = sessionParser.parse(s"$strContent", "dd.MM.yyyy_HH:mm:ss");
      val fileSession = FileSession(filePath, session)

      Some(fileSession)
    }

    // Кэширование
    // Не должно работать?
    sessionEvents.cache()

    // Разворачиваем список searchEvents из каждого SessionEvent
    val docsWithId = sessionEvents.flatMap { fs =>
      // Оставляем только CardSearchEvent
      // и разворачиваем список docsOpened из каждого CardSearchEvent
      fs.sessionEvent.searchEvents.filter {
        case _: CardSearchEvent => true
        case _ => false
      }.flatMap { cardSearch =>
        cardSearch.docsOpened.filter(docOpen =>
          docOpen.docId.equals(searchingId)
        )
      }
    }

    // Количество документов с нужным идентификатором
    val docsCount = docsWithId.count()

    // Создание RDD
    val resultRDD = spark.sparkContext.parallelize(Seq(s"Count_$searchingId: $docsCount"))
    // Сохранения результата в файл
    resultRDD.coalesce(1).saveAsTextFile(outputFile)

    spark.stop()
  }

  /**
   * Подсчет количества открытий каждого документа, найденного через быстрый поиск за каждый день
   *
   * @param sessionsFiles   Файл или директория с входными данными
   * @param datetimePattern Шаблон даты-времени
   * @param outputFile      Директория с выходными данными
   */
  def countQuickByDate(sessionsFiles: String, datetimePattern: String, outputFile: String): Unit = {
    // Создание spark сессии
    val spark = SparkSession.builder
      .appName("Count card searches")
      .getOrCreate()
    // Подключение приведений данных
    import spark.implicits._
    // Чтение файлов побайтово
    val binaryFilesDF = spark.read
      .format("binaryFile")
      .load(sessionsFiles)

    // Получение из битовых массивов списка объектов сессий
    val sessionEvents = binaryFilesDF.flatMap { file =>
      val filePath = file.getString(0)
      val strContent = new String(
        file.get(3).asInstanceOf[Array[Byte]],
        Charset.forName("windows-1251")
      )
      val sessionParser: SessionEventParser = new SessionEventParser()
      val session = sessionParser.parse(s"$strContent", "dd.MM.yyyy_HH:mm:ss");
      val fileSession = FileSession(filePath, session)

      Some(fileSession)
    }

    // Разворачиваем список searchEvents из каждого SessionEvent
    val docsCountByDate = sessionEvents.flatMap { fs =>
      // Оставляем только QsEvent
      // и разворачиваем список docsOpened из каждого CardSearchEvent
      fs.sessionEvent.searchEvents.filter {
        case _: QsEvent => true
        case _ => false
      }.flatMap { cardSearch =>
        cardSearch.docsOpened
      }
    }.groupBy(date_format($"timestamp", "yyyy-MM-dd").alias("date")).count()

    // Запись в csv-файл
    docsCountByDate.coalesce(1).write.mode("overwrite").option("header", "true").csv(outputFile)

    spark.stop()
  }
}
