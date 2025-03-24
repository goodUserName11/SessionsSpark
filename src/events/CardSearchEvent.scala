package events

import common.Problem

import java.sql.Timestamp

/**
 * Карточка поиска
 * @param timestamp дата и время события
 * @param docsFound найденные документы
 * @param docsOpened открытые документы
 * @param queryParams параметры поиска
 * @param searchId идентификатор поиска
 * @param parsingProblems найденные проблемы про парсинге
 * @param name имя события
 */
case class CardSearchEvent(
  override val timestamp: Timestamp,
  docsFound: List[String],
  docsOpened: List[DocOpenEvent],
  queryParams: Map[String, String],
  searchId: String,
  override val parsingProblems: List[Problem],
  override val name: String = "CARD_SEARCH"
) extends SearchEvent
