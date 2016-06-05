package de.oweissbarth

import org.junit._
import Assert._
import de.oweissbarth.sample.{Category, CategorySet}

class CategoryTest {
  @Test
  def testCategory()= {
    val cs = new CategorySet()
    val c1 = cs.get("Hund")
    println(c1)
    val c2 = cs.get("Katze")
    println(c2)

    val c3 = cs.get("Fisch")
    println(c3)


    val c4 = cs.get("Hund")
    println(c4)

    val c5 = cs.get("Fisch")
    println(c5)



    assertTrue(c1 != c2)
    assertTrue(c1 != c3)
    assertTrue(c1 == c4)
    assertTrue(c1 != c5)

    assertTrue(c2 != c3)
    assertTrue(c2 != c4)
    assertTrue(c2 != c5)

    assertTrue(c3 != c4)
    assertTrue(c3 == c5)

    assertTrue(c4 != c5)

    assertEquals(3, cs.size)
  }
}
