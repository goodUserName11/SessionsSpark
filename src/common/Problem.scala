package common

/**
 * Хранение информации о проблемах
 * @param lineNumber номер строки, в которой есть проблема
 * @param description описание проблемы
 * @param line строка с проблемой
 */
case class Problem(lineNumber: Int, description: String, line: String)
