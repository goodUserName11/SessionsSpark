package common

import events.{AbstractEvent, CardSearchEvent, QsEvent, SearchEvent, SessionEvent}
import org.apache.spark.sql.Encoders

/**
 * Создание энкодеров для сериализации объектоа
 */
object EventsEncoders {
  implicit val abstractEventEncoder = Encoders.kryo[AbstractEvent]
  implicit val sessionEventEncoder = Encoders.kryo[SessionEvent]
  implicit val searchEventEncoder = Encoders.kryo[SearchEvent]
  implicit val qsEventEncoder = Encoders.kryo[QsEvent]
  implicit val cardSearchEventEncoder = Encoders.kryo[CardSearchEvent]
}
