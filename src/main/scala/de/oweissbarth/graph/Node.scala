package de.oweissbarth.graph

import de.oweissbarth.model._
import de.oweissbarth.sample._

class Node(val label: String, var parents: List[Node], var modelProvider: Option[ModelProvider], var dataSet: Option[DataSet]){
	var dirty = true

	private val categoricalParents : List[Node] = parents.filter(e => (e.dataSet != None && e.dataSet.get.isInstanceOf[CategoricalDataSet]))
	private val categoryCombinations = categoricalParents.map(p=>p.dataSet.asInstanceOf[CategoricalDataSet].categories.size).foldLeft(1)((a,b)=>a*b)// NOTE use pattern matching here?

	val models = new Array[Model](categoryCombinations)

	def this(label:String) = {
		this(label, List(), None , None)
	}
	
	override def toString() = {
		"Node: "+label+" [parents: " + parents.map(_.toString()) + "]"
	}


	def fit()= {
		if(dirty){
			for(i <- 0 until models.length){
				models(i) = computeModel(i)
			}
		}
	}

	private def computeModel(combination: Int): Model = {
		modelProvider match{
			case Some(modelProvider) => modelProvider.getModel()

			case None => throw new Exception("No modelprovider defined")
		}
	}

}
