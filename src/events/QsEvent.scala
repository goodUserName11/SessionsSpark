package events
import common.Problem

import java.sql.Timestamp

/**
 * Быстрый поиск
 * @param timestamp дата и время события
 * @param docsFound найденные документы
 * @param docsOpened открытые документы
 * @param query поисковой запрос
 * @param searchId идентификатор поиска
 * @param parsingProblems найденные проблемы про парсинге
 * @param name имя события
 */
case class QsEvent(
  override val timestamp: Timestamp,
  docsFound: List[String],
  docsOpened: List[DocOpenEvent],
  query: String,
  searchId: String,
  override val parsingProblems: List[Problem],
  override val name: String = "QS"
) extends SearchEvent
