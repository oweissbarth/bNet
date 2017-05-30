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

package de.oweissbarth.bnet.core

import de.oweissbarth.bnet.graph._
import de.oweissbarth.bnet.sample._
import de.oweissbarth.bnet.model._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.json4s.jackson.JsonMethods._

/** The full bayesian network
  *
  * When initialized this constructs a new spark context.
  *
  * @todo In the future it should be possible to pass an existing spark context as a parameter
  *
  * @constructor creates a new bayesian network
  *
  * @param graph the graph for the network
  */
class BayesianNetwork(private[oweissbarth] val graph: DirectedAcyclicGraph){

  private val sparkConf = new SparkConf().setAppName("bayesian").setMaster("local")

  private val sc = SparkContext.getOrCreate(sparkConf)

  private val sqlc = new SQLContext(sc)


  private val logger = LogManager.getRootLogger

  logger.setLevel(Level.WARN)


  /*logger.info("trying to match columns and nodes...")
  logger.info(sample.records.schema)

  { r => if(graph.nodes.contains(r.label)){
                                    logger.info("matched column "+d.label+" to node with same name")
                                    graph.nodes(d.label).dataSet = Some(d)
                                  }else{ 
                                    logger.warn("Could not match column "+d.label+" to a node")}
                          }*/
  logger.info("Graph built.")

  /** Constructs a BayesianNetwork from a GraphProvider
    *
    * @param graphProvider supplies the graph for the BayesianNetwork
    */
  def this(graphProvider: GraphProvider) = {
    this(graphProvider.getGraph())
  }

  /** Models all nodes in the bayesian network
    *
    * @note Modelproviders for each node need to be specfied before running fit.
    *
    * @param sp supplies the data sample to model against
    */
  def fit(sp: SampleProvider):Unit = {
    graph.nodes.values.map(_.fit(sp.getSample().records))
  }

  /** specifies how to model a specific node
    *
    * @param label the node's label
    * @param modelType describes how a node is modelled
    */
  def setModelType(label:String, modelType: ModelProvider) ={
    graph.nodes(label).modelProvider = Some(modelType)
  }

  /** returns the model type for a specific node
    *
    * @param label the node's label
    * @return the nodes model provider
    */
  def getModelType(label:String):ModelProvider ={
    graph.getNodeByLabel(label).modelProvider.get
  }

  /** returns the model of a given node as json
    *
    * @param label the node's label
    * @return the model asJson
    */
  def getModel(label :String) = {
    graph.getNodeByLabel(label).model.get.asJson()
  }

  /** returns a json representation of the bayesian network
    *
    * @return a json representation of the bayesian network
    */
  def asJson() = {
    graph.asJson()
  }


  def copy() = {
    new BayesianNetwork(this.graph.copy())
  }

  /** ends the bayesian network and stops the spark context
    *
    */
  def close() = {
    sc.stop()
  }


}

object BayesianNetwork{
  var modelTypes: List[Class[_]] = List(classOf[SimpleLinearModel], classOf[SimpleCategoricalModel], classOf[GaussianBaseModel], classOf[SimpleLinearModelParameterSet])

  def fromJson(json: String): BayesianNetwork = {
    val ast = parse(json)

    //TODO check header

    val graph = DirectedAcyclicGraph.fromJson(ast.children(0))


    new BayesianNetwork(graph)
  }

  def registerModelType(c: Class[_<:Model]) = {
    modelTypes ::= c
  }
}