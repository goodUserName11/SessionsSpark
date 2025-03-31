import events._
import parsers.SessionEventParser
import common.EventsEncoders._
import common.FileSession
import org.apache.spark.sql.SparkSession

import java.nio.charset.Charset
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions._



/*
TODO Переписать код:
 1. +todo Читать документы спарком побайтово
 2. +todo Инкапсулировать парсинг (выделить в классы, отдельный класс на каждое событие)
 3. todo Переписать код по SOLID
 4. todo Покрыть тестами
 5. +todo Изменить Подсчет количества раз, когда в карточке производили поиск документа с указанным идентификатором
 6. +-todo Поиск ошибок и проблем парсинга (сохранть проблемы в переменную)
*/
object NewMain {
  def main(args: Array[String]): Unit ={
    // Чтение путей из параметров
    //val sessionsFile = args(0)
    val sessionsFiles = "/Сессии/"
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val searchingId = "ACC_45616"
    val countOutput = "/opt/spark/work-dir/consres/count.txt"
    val countByDateOutput = "/opt/spark/work-dir/consres/countByDate.txt"

    val spark = SparkSession.builder
      .appName("Count card searches")
      .getOrCreate()
    import spark.implicits._
    // Чтение файлов побайтово
    val binaryFilesDF = spark.read.format("binaryFile").load(sessionsFiles)

    // Показать схему DataFrame
    binaryFilesDF.printSchema()
    //root
    // |-- path: string (nullable = true)
    // |-- modificationTime: timestamp (nullable = true)
    // |-- length: long (nullable = true)
    // |-- content: binary (nullable = true)

    var count: Int = 0
    // Пример обработки данных по строчкам
    binaryFilesDF.rdd.foreach { row =>
      val filePath = row.getString(0) // Путь к файлу
      val content = row.get(3) // Содержимое файла в виде массива байтов
      count += 1
      println(s"$count. Файл: $filePath, размер: ${content.asInstanceOf[Array[Byte]].length} байтов")
    }

    // Преобразуем массив байтов в строку с использованием Windows-1251
//    val str: String = new String(byteArray, Charset.forName("windows-1251"))



    // todo: Перетащить в отдельный класс
    // Создание encoder-ов для сериализации объектов
    implicit val abstractEventEncoder = Encoders.kryo[AbstractEvent]
    implicit val sessionEventEncoder = Encoders.kryo[SessionEvent]
    implicit val searchEventEncoder = Encoders.kryo[SearchEvent]
    implicit val qsEventEncoder = Encoders.kryo[QsEvent]
    implicit val cardSearchEventEncoder = Encoders.kryo[CardSearchEvent]

    val sessionEvents = binaryFilesDF.flatMap{file =>
      val filePath = file.getString(0)
      val strContent = new String(file.get(3).asInstanceOf[Array[Byte]], Charset.forName("windows-1251"))
      val sessionParser: SessionEventParser = new SessionEventParser()
      val session = sessionParser.parse(s"$strContent", "dd.MM.yyyy_HH:mm:ss");
      val fileSession = FileSession(filePath, session)

      Some(fileSession)
//      match {
//        case Some(event) => Some(event)
//      }
    }

    sessionEvents.foreach(fs => println(s"${fs.sessionEvent.name}: ${fs.fileName}: ${fs.sessionEvent.searchEvents(0).name}\n"))

    spark.stop()
  }

  /**
   * Подсчет количества раз, когда в карточке производили поиск документа с указанным идентификатором
   *
   * @param sessionsFiles Файл или директория с входными данными
   * @param datetimePattern Шаблон даты-времени
   * @param outputFile   Директория с выходными данными
   * @param searchingId  Идентификатор для подсчета
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

    // todo: Перетащить в отдельный класс
    // Создание encoder-ов для сериализации объектов
//    implicit val abstractEventEncoder = Encoders.kryo[AbstractEvent]
//    implicit val sessionEventEncoder = Encoders.kryo[SessionEvent]
//    implicit val searchEventEncoder = Encoders.kryo[SearchEvent]
//    implicit val qsEventEncoder = Encoders.kryo[QsEvent]
//    implicit val cardSearchEventEncoder = Encoders.kryo[CardSearchEvent]

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
      }.flatMap {cardSearch =>
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
   * @param sessionsFiles Файл или директория с входными данными
   * @param datetimePattern Шаблон даты-времени
   * @param outputFile   Директория с выходными данными
   */
  def countQuickByDate(sessionsFiles: String, datetimePattern: String, outputFile: String): Unit ={
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

    // todo: Перетащить в отдельный класс
    // Создание encoder-ов для сериализации объектов
//    implicit val abstractEventEncoder = Encoders.kryo[AbstractEvent]
//    implicit val sessionEventEncoder = Encoders.kryo[SessionEvent]
//    implicit val searchEventEncoder = Encoders.kryo[SearchEvent]
//    implicit val qsEventEncoder = Encoders.kryo[QsEvent]
//    implicit val cardSearchEventEncoder = Encoders.kryo[CardSearchEvent]

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
