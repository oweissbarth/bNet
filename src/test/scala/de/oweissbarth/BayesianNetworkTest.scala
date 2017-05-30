/*
 * Copyright 2017 Oliver Weissbarth
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package de.oweissbarth


import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.model.{GaussianBaseModel, GaussianBaseModelProvider, SimpleCategoricalModelProvider, SimpleLinearModelProvider}
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

  it should "skip nodes with no modelprovider when fitting" in {
    val sp = new CSVSampleProvider("src/test/resources/ageGenderIncome.csv", ";")
    val gp = new GraphMLGraphProvider("src/test/resources/ageGenderIncome.gml")
    val bn = new BayesianNetwork(gp)
    bn.fit(sp)
  }

  it should "copy correctly" in{
    val sp = new CSVSampleProvider("src/test/resources/ageGenderIncome.csv", ";")

    val gp = new GraphMLGraphProvider("src/test/resources/ageGenderIncome.gml")

    val bn = new BayesianNetwork(gp)

    val slmp = new SimpleLinearModelProvider()
    val gbmp = new GaussianBaseModelProvider()
    val scmp = new SimpleCategoricalModelProvider()

    bn.setModelType("Age", gbmp)
    bn.setModelType("Gender", scmp)
    bn.setModelType("Income", slmp)

    val bn2 = bn.copy()

    bn.setModelType("Age", new SimpleLinearModelProvider())

    bn2.getModelType("Age") shouldBe a [GaussianBaseModelProvider]
    bn.getModelType("Age") shouldBe a [SimpleLinearModelProvider]

    bn.close()
    bn2.close()

  }
}