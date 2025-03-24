package parsers

import events.AbstractEvent

/**
 * Общий интерфейс для всех парсеров событий
 */
trait AbstractEventParser {
  /**
   * Метод парсинга строки в событие
   * @param str строка для парсинга
   * @param datetimePattern шаблон даты для парсинга
   * @return событие
   */
  def parse(str: String, datetimePattern: String): AbstractEvent
}
