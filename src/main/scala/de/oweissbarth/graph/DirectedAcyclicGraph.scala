package de.oweissbarth.graph

import scala.collection.immutable.HashMap;

class DirectedAcyclicGraph{

	var nodes: HashMap[String, Node] = HashMap()
	
	/**Creates a Graph from a list of node labels and edges
	 * 
	 * @param labels a List of node names
	 * @param edges a list of int tuples describing the edges (zero indexed)
	 */
	def fromLabelsAndEdges(labels: List[String], edges: List[(Int, Int)]) = {

		val nodes = labels.map(new Node(_))
		this.nodes = HashMap(labels.zip(nodes): _*)
		val parentsIDs = edges.groupBy(_._2).map({case (key, value)=> (key, value.map(_._1))})
		val allParentsIDs = (0 to nodes.length-1).map(parentsIDs.get(_)).map({case(e)=>e.getOrElse(List[Int]())}).toList
		val allParentNodes = allParentsIDs.map({case(e) => e.map({case(a)=>nodes(a)})})
		
		val zippedList = nodes.zip(allParentNodes)
		zippedList.map({case (node, hisParentNodes) => node.parents = hisParentNodes.toArray})
	}
	
	def getNodeByLabel(label: String) = {
	  
	}
	
	
	/**checks if the graph is acyclic
	 * 
	 * TODO make this more efficient
	 * 
	 */
	def isValid():Boolean = {
	  def visit(nodes: Array[Node], known: List[Node]):Boolean={
	    if(nodes.intersect(known).length != 0){
	      false
	    }else{
	      nodes.map({
    	    case(node)=>
    	      visit(node.parents, node::known)  
    	    }).foldLeft(true)(_ && _)
	    }
	    
	  }
	  visit(nodes.values.toArray, List())
	}
	
	override def toString():String={
	  "DirectedAcyclicGraph: " + this.nodes
	}
}
