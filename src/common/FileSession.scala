package common

import  events.SessionEvent

/**
 * Связь сессии с файлом
 * @param fileName название файла
 * @param sessionEvent сессия
 */
case class FileSession(fileName: String, sessionEvent: SessionEvent)
