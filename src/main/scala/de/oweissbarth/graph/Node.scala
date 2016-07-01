package de.oweissbarth.graph

import org.apache.log4j.LogManager
import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.model._
import de.oweissbarth.sample._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

/** a node in a DirectedAcyclicGraph
  *
  * @todo remove dataSet parameter
  *
  * @constructor creates a new node
  *
  * @param label the label of the node
  * @param parents an array containing the parent nodes
  * @param dataSet
  */
class Node(val label: String, var parents: Array[Node], var dataSet: Option[Record]){
	var dirty = true
  var model :Option[Model]= None
  var modelProvider : Option[ModelProvider] = None

	// UNUSED
	var nodeType = BayesianNetwork.NONE


  /** creates a new node just from a label
    *
    * @param label the nodes label
    *
    * @todo remove this constructor. make parents val
    */
	def this(label:String) = {
		this(label, Array(), None)
	}

  /** returns a human readable representation of the node
    *
    * @return a human readable representation of the node
    */
	override def toString() = {
		"Node: "+label+" [parents: " + parents.map(_.toString()) + "]"
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

}
