package events

import common.Problem

import java.sql.Timestamp

/**
 * Открытие документа
 * @param timestamp дата и время события
 * @param searchId идентификатор поиска
 * @param docId идентификатор документа
 * @param parsingProblems найденные проблемы про парсинге
 * @param name имя события
 */
case class DocOpenEvent(
  override val timestamp: Timestamp,
  searchId: String,
  docId: String,
  override val parsingProblems: List[Problem],
  override val name: String = "DOC_OPEN"
) extends AbstractEvent

