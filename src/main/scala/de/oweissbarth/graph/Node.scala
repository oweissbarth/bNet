package de.oweissbarth.graph

import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.model._
import de.oweissbarth.sample._

class Node(val label: String, var parents: List[Node], var dataSet: Option[Record]){
	var dirty = true
  var model :Option[Model]= None
  var modelProvider : Option[ModelProvider] = None

	// UNUSED
	var nodeType = BayesianNetwork.NONE


	def this(label:String) = {
		this(label, List(), None)
	}
	
	override def toString() = {
		"Node: "+label+" [parents: " + parents.map(_.toString()) + "]"
	}


	def fit()= {

	}

	/*private def computeModel(combination: Int): Model = {
		modelProvider match{
			case Some(modelProvider) => modelProvider.getModel()

			case None => throw new Exception("No modelprovider defined")
		}
	}*/

}
