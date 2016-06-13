package de.oweissbarth


import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.model.SimpleLinearModelProvider
import de.oweissbarth.graph.GraphMLGraphProvider
import de.oweissbarth.sample.CSVSampleProvider

import org.scalatest._



class BayesianNetworkTest extends FlatSpec with Matchers {

  "A BayesianNetwork" should "be constructable from graph and Sample" in {
    val gp = new GraphMLGraphProvider("src/test/resources/xy_graph.gml")
    assert(gp != null)
    val sp = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")
    assert(sp != null)

    val bn = new BayesianNetwork(gp, sp)

    assert(bn != null)

    bn.close()

  }


  it should "have a working method to set and retrieve the model type per node " in {
    val gp = new GraphMLGraphProvider("src/test/resources/xy_graph.gml")
    assert(gp != null)
    val sp = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")
    assert(sp != null)

    val bn = new BayesianNetwork(gp, sp)

    assert(bn != null)


    val lnMP = new SimpleLinearModelProvider()

    bn.setModelType("x", lnMP)

    bn.getModelType("x") should be (lnMP)

    bn.close()
  }

  it should "have a working method to set and retrieve the node type" in {
    val gp = new GraphMLGraphProvider("src/test/resources/genderAgeIncome.gml")
    assert(gp != null)
    val sp = new CSVSampleProvider("src/test/resources/genderAgeIncome.csv", ",")
    assert(sp != null)

    val bn = new BayesianNetwork(gp, sp)

    assert(bn != null)

    bn.setNodeType("Age", BayesianNetwork.INTERVAL)
    bn.setNodeType("Gender", BayesianNetwork.CATEGORICAL)
    bn.setNodeType("Income", BayesianNetwork.INTERVAL)

    bn.getNodeType("Age") should be (BayesianNetwork.INTERVAL)
    bn.getNodeType("Gender") should be (BayesianNetwork.CATEGORICAL)
    bn.getNodeType("Income") should be (BayesianNetwork.INTERVAL)

    bn.close()
  }
}