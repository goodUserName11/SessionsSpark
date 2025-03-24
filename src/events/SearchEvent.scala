package events

import common.Problem

import java.sql.Timestamp

/**
 * Абстракное событие поиска
 */
trait SearchEvent extends AbstractEvent {
 /**
  * Найденные документы
  */
 val docsFound: List[String]
 /**
  * Открытые документы
  */
 val docsOpened: List[DocOpenEvent]
 /**
  * Идентификатор поиска
  */
 val searchId: String
}
