import common.EventsEncoders._
import spark.SparkTasks._

object Main {
  def main(args: Array[String]): Unit = {
    val sessionsFiles = "/Сессии/"
    val datetimePattern = "dd.MM.yyyy_HH:mm:ss"
    val searchingId = "ACC_45616"
    val countOutput = "/opt/spark/work-dir/consres/count.txt"
    val countByDateOutput = "/opt/spark/work-dir/consres/countByDate.csv"

    countSearches(sessionsFiles, datetimePattern, countOutput, searchingId)
    countQuickByDate(sessionsFiles, datetimePattern, countByDateOutput)
  }
}
