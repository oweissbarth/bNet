package de.oweissbarth

import org.junit._
import Assert._

import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.model.LinearModelProvider
import de.oweissbarth.graph.GraphMLGraphProvider
import de.oweissbarth.sample.CSVSampleProvider


@Test
class BayesianNetworkTest {
  @Test
  def testNetworkCreation() ={

    val gp = new GraphMLGraphProvider("src/test/resources/xy_graph.gml")
    assertNotNull(gp)
    val sp = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")
    assertNotNull(sp)
    
    val bn = new BayesianNetwork(gp, sp)
    
    assertNotNull(bn)
    
    val lnMP = new LinearModelProvider()
    
    bn.setModelType("x", lnMP)
    
    assertEquals(lnMP, bn.getModelType("x"))
    
    bn.fit()
    
  }

  @Test
  def testNetworkCreationWithCategorical() ={
    val gp = new GraphMLGraphProvider("src/test/resources/genderAgeIncome.gml")
    assertNotNull(gp)
    val sp = new CSVSampleProvider("src/test/resources/genderAgeIncome.csv", ",")
    assertNotNull(sp)

    val bn = new BayesianNetwork(gp, sp)

    assertNotNull(bn)

    assertEquals(BayesianNetwork.INTERVAL, bn.getColumnType("Age"))
    assertEquals(BayesianNetwork.CATEGORICAL, bn.getColumnType("Gender") )
    assertEquals(BayesianNetwork.INTERVAL, bn.getColumnType("Income"))


    bn.fit()

  }
}