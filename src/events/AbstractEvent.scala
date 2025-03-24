package events

import common.Problem

import java.sql.Timestamp

/**
 * Базовый класс для события
 */
trait AbstractEvent {
  val name: String
  val timestamp: Timestamp
  val parsingProblems: List[Problem]
}