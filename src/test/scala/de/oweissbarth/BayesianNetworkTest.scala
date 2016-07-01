package de.oweissbarth


import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.model.{GaussianBaseModelProvider, SimpleCategoricalModelProvider, SimpleLinearModelProvider}
import de.oweissbarth.graph.{GraphMLGraphProvider, Node}
import de.oweissbarth.sample.CSVSampleProvider
import org.apache.log4j.LogManager
import org.scalatest._



class BayesianNetworkTest extends FlatSpec with Matchers {

  "A BayesianNetwork" should "be constructable from graph and Sample" in {
    val gp = new GraphMLGraphProvider("src/test/resources/xy_graph.gml")
    assert(gp != null)

    val bn = new BayesianNetwork(gp)

    assert(bn != null)

    bn.close()

  }


  it should "have a working method to set and retrieve the model type per node " in {
    val gp = new GraphMLGraphProvider("src/test/resources/xy_graph.gml")
    assert(gp != null)

    val bn = new BayesianNetwork(gp)

    assert(bn != null)


    val lnMP = new SimpleLinearModelProvider()

    bn.setModelType("x", lnMP)

    bn.getModelType("x") should be (lnMP)

    bn.close()
  }

  it should "run fit without errors " in {
    val gp = new GraphMLGraphProvider("src/test/resources/xy_graph.gml")
    assert(gp != null)
    val sp = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")
    assert(sp != null)

    val bn = new BayesianNetwork(gp)

    assert(bn != null)

    bn.setModelType("x", new GaussianBaseModelProvider())
    bn.setModelType("y", new SimpleLinearModelProvider())

    bn.fit(sp)

    bn.close()
  }

  it should "fit the full network and return correct models" in{
    val logger = LogManager.getLogger(s"Fitting BaysianNetwork")



    val sp = new CSVSampleProvider("src/test/resources/ageGenderIncome.csv", ";")

    val gp = new GraphMLGraphProvider("src/test/resources/ageGenderIncome.gml")

    val bn = new BayesianNetwork(gp)

    val slmp = new SimpleLinearModelProvider()
    val gbmp = new GaussianBaseModelProvider()
    val scmp = new SimpleCategoricalModelProvider()

    bn.setModelType("Age", gbmp)
    bn.setModelType("Gender", scmp)
    bn.setModelType("Income", slmp)

    bn.fit(sp)

    logger.warn(bn.getModel("Age"))
    logger.warn(bn.getModel("Gender"))
    logger.warn(bn.getModel("Income"))


    bn.close()

  }
}