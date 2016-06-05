package de.oweissbarth


import org.junit._
import Assert._
import de.oweissbarth.sample._

@Test
class SampleTest {
  @Test
  def testCSVSampleProvider()={
    val prov = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")
    assertNotNull(prov)
    assertNotNull(prov.getSample())
  }
  
  @Test
  def testFields() ={
    val interval = new IntervalField(2.4f)
    assertNotNull(interval)
  }
  
  @Test
  def testSets() ={
    val interval1 = new IntervalField(2.4f)
    val interval2 = new IntervalField(7.6f)
    val interval3 = new IntervalField(6.9f)
    val interval4 = new IntervalField(5.7f)
    
    val list = List(interval1, interval2, interval3, interval4)
    
    val set = new IntervalDataSet(list, "testing")
    
    assertNotNull(set)

  }

  @Test
  def testMultiTypeCSV() = {
    val prov = new CSVSampleProvider("src/test/resources/genderAgeIncome.csv", ",")

    val sets = prov.getSample().dataSets

    assertTrue(sets(0).isInstanceOf[CategoricalDataSet])
    assertTrue(sets(1).isInstanceOf[IntervalDataSet])
    assertTrue(sets(2).isInstanceOf[IntervalDataSet])

  }



}