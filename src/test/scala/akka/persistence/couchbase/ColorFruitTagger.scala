package akka.persistence.couchbase

import akka.persistence.journal.{Tagged, WriteEventAdapter}

class ColorFruitTagger extends WriteEventAdapter {
  val colors = Set("green", "black", "blue", "yellow", "pink")
  val fruits = Set("apple", "banana", "kiwi")
  override def toJournal(event: Any): Any = event match {
    case s: String =>
      val colorTags = colors.foldLeft(Set.empty[String])((acc, c) => if (s.contains(c)) acc + c else acc)
      val fruitTags = fruits.foldLeft(Set.empty[String])((acc, c) => if (s.contains(c)) acc + c else acc)
      val tags = colorTags union fruitTags
      if (tags.isEmpty) event
      else Tagged(event, tags)
    case _ => event
  }

  override def manifest(event: Any): String = ""
}

