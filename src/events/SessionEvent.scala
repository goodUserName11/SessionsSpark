package events

import common.Problem

import java.sql.Timestamp
import scala.collection.mutable.MutableList

/**
 * Сессия
 * @param timestamp дата и время события
 * @param endTimestamp дата и время окончания сессии
 * @param searchEvents список событий поиска
 * @param parsingProblems найденные проблемы про парсинге
 * @param name имя события
 */
case class SessionEvent(
  override val timestamp: Timestamp,
  endTimestamp: Timestamp,
  searchEvents: List[SearchEvent],
  override val parsingProblems: List[Problem],
  override val name: String = "SESSION"
) extends AbstractEvent