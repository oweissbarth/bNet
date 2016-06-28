package de.oweissbarth.graph

import org.apache.log4j.LogManager
import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.model._
import de.oweissbarth.sample._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import de.oweissbarth.model.CategoricalModelProvider

class Node(val label: String, var parents: Array[Node], var dataSet: Option[Record]){
	var dirty = true
  var model :Option[Model]= None
  var modelProvider : Option[ModelProvider] = None

	// UNUSED
	var nodeType = BayesianNetwork.NONE


	def this(label:String) = {
		this(label, Array(), None)
	}
	
	override def toString() = {
		"Node: "+label+" [parents: " + parents.map(_.toString()) + "]"
	}


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
