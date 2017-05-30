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

package de.oweissbarth.bnet.graph

import org.apache.log4j.LogManager
import de.oweissbarth.bnet.core.BayesianNetwork
import de.oweissbarth.bnet.model._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/** a node in a DirectedAcyclicGraph
  *
  *
  * @constructor creates a new node
  *
  * @param label the label of the node
  * @param parents an array containing the parent nodes
  */
class Node(val label: String, var parents: Array[Node]){
	var dirty = true
  var model :Option[Model]= None
  var modelProvider : Option[ModelProvider] = None


  /** creates a new node just from a label
    *
    * @param label the nodes label
    *
    * @todo remove this constructor. make parents val
    */
	def this(label:String) = {
		this(label, Array())
	}


  /** creates a new node from label, parents and model
    *
    * @note this is mainly used when loading a graph from a file
    *
    * @param label the nodes label
    * @param parents the parent nodes
    * @param model the model for this node
    */
  def this(label: String, parents: Array[Node], model: Model, modelProvider: ModelProvider) = {
    this(label, parents)
    this.model = Option(model)
    this.modelProvider = Option(modelProvider)
  }

  /** returns a human readable representation of the node
    *
    * @return a human readable representation of the node
    */
	override def toString() = {
		"Node: "+label+" [parents: " + parents.map(_.label).foldLeft("")( _+","+_) + "]"
	}

  /** creates a model based on the specified modelProvider and sample data
    *
    * @todo move modelprovider to BayesianNetwork and pass it here a parameter
    * @todo the selection of the nessecary columns should be in the bayesian network
    * @param sample the sample data to model against
    */
	def fit(sample: DataFrame)= {
   val logger = LogManager.getLogger(s"Fitting for $label")
    val sqlc = SQLContext.getOrCreate(SparkContext.getOrCreate())
    if(dirty && modelProvider.isDefined){
      val subDataSet = sample.select(label, parents.map(b=>b.label):_*)
      model = Some(modelProvider.get.getModel(subDataSet, parents))
      dirty = false
    }else{
      logger.warn("No Modelprovider specified. skipping")
    }
	}

  /** returns true if the node is categorical
    *
    * @note also returns false if model type could not be determined
    *
    * @return true if has categorical model or modelprovider
    */
  def isCategorical() = {
    if(model.isDefined)
      model.get.isInstanceOf[CategoricalModel]
    else if(modelProvider.isDefined)
      modelProvider.get.isInstanceOf[CategoricalModelProvider]
    else
      false
  }

  /** returns a json representation of the node
    *
    * @return  a json represenation of the node
    */
  def asJson() = {
    s"""{"label": "$label", "parents": [${if(parents.nonEmpty) parents.map('"'+_.label+'"').reduce(_+", "+ _)else ""}], "model": ${if(model.isDefined) model.get.asJson() else "null"}}"""
  }

}
