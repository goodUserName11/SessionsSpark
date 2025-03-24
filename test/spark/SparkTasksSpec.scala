package spark

import spark.SparkTasks._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.nio.file.{Files, Paths}

class SparkTasksSpec extends AnyFlatSpec with Matchers {
  "SparkTasks" should "create a file and correctly count searches" in {
    val sessionsFiles = "/Сессии/6"
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val searchingId = "PAP_70372"

    // Временная директория
    val tempOutputDir = Files.createTempDirectory("/opt/spark/work-dir/temp/").toString
    val outputFilePath = s"$tempOutputDir/count.txt"

    countSearches(sessionsFiles, datetimePattern, outputFilePath, searchingId)

    // Проверяем, что файл был создан
    Files.exists(Paths.get(outputFilePath)) should be(true)

    // Проверяем содержимое файла
    val content = Files
      .readAllLines(Paths.get(outputFilePath))
      .toArray
      .mkString("")

    content should include (s"Count_$searchingId: 1")

    // Удаляем временную директорию после теста
    Files.deleteIfExists(Paths.get(outputFilePath))
    Files.deleteIfExists(Paths.get(tempOutputDir))
  }

  it should "create a file and correctly quick counts by date" in {
    val sessionsFiles = "/Сессии/6"
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val shouldInclude =
      """date,count
        |2020-06-13,6""".stripMargin

    // Временная директория
    val tempOutputDir = Files.createTempDirectory("/opt/spark/work-dir/temp/").toString
    val outputFilePath = s"$tempOutputDir/countByDate.txt"

    countQuickByDate(sessionsFiles, datetimePattern, outputFilePath)

    // Проверяем, что файл был создан
    Files.exists(Paths.get(outputFilePath)) should be(true)

    // Проверяем содержимое файла
    val content = Files
      .readAllLines(Paths.get(outputFilePath))
      .toArray
      .mkString("\n")

    content should include(shouldInclude)

    // Удаляем временную директорию после теста
    Files.deleteIfExists(Paths.get(outputFilePath))
    Files.deleteIfExists(Paths.get(tempOutputDir))
  }
}
