package de.oweissbarth.graph

import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.model.{GaussianBaseModel, Model, SimpleCategoricalModel, SimpleLinearModel}
import org.json4s.{DefaultFormats, ShortTypeHints}
import org.json4s.JsonAST.JValue

import scala.collection.immutable.HashMap
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

/** a graph with directed edges and without cycles
  *
  * @constructor creates a new empty graph
  */
class DirectedAcyclicGraph(var nodes: HashMap[String, Node]= HashMap()){


	/**Creates a Graph from a list of node labels and edges
	 *
	 * @param labels a List of node names
	 * @param edges a list of int tuples describing the edges (zero indexed)
	 */
	def fromLabelsAndEdges(labels: List[String], edges: List[(Int, Int)]): DirectedAcyclicGraph = {

		val nodes = labels.map(new Node(_))
		this.nodes = HashMap(labels.zip(nodes): _*)
		val parentsIDs = edges.groupBy(_._2).map({case (key, value)=> (key, value.map(_._1))})
		val allParentsIDs = (0 to nodes.length-1).map(parentsIDs.get(_)).map({case(e)=>e.getOrElse(List[Int]())}).toList
		val allParentNodes = allParentsIDs.map({case(e) => e.map({case(a)=>nodes(a)})})

		val zippedList = nodes.zip(allParentNodes)
		zippedList.map({case (node, hisParentNodes) => node.parents = hisParentNodes.toArray})
    this
	}

  /** returns the node with a given label
    *
    * @param label the node's label
    * @return the node with the given label
    */
	def getNodeByLabel(label: String): Node = {
	  nodes(label)
	}


  /** checks if the graph is acyclic
		*
		* @todo make this more efficient
		* @return true if the graph is acyclic, false if not
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

  /** Creates a human readable representation of the graph
    *
    * @return a string representation of the graph
    */
	override def toString():String={
	  "DirectedAcyclicGraph: " + this.nodes
	}

  /** returns a json representation of the dag
    *
    * @return a json representation of the node
    */
    def asJson() = {
      s"""{"BayesianNetwork": [\n\t${nodes.map(_._2.asJson()).reduce(_+",\n\t"+_)}\n]}"""
    }

  /** returns a copy of the graph. Nodes are immutable so not copied.
    *
    * @return
    */
	def copy() = {
    new DirectedAcyclicGraph()
  }
}

object DirectedAcyclicGraph{
  def fromJson(ast: JValue): DirectedAcyclicGraph = {
    implicit  val formats = new DefaultFormats{
      override val typeHints = ShortTypeHints(BayesianNetwork.modelTypes)
      override val typeHintFieldName = "type"
    }

    //temporary node storage
    case class JsonNode(label: String, parents: Array[String], model: Model)

    val tmpNodes = for(c <- ast.children)yield  c.extract[JsonNode]

    //sort nodes to build the graph
    val leafs = tmpNodes.filter(_.parents.isEmpty).map(x=>(x.label, new Node(x.label, Array(), x.model))).toMap

    // NOTE doing a topological sort might be faster here
    def createNodesWithDeps(deps: Map[String, Node], open:List[JsonNode]):Map[String,Node] = {
      if(open.isEmpty)
        deps
      else{
        //split open nodes into nodes with satified dependecies and otherwise
        val (current, newOpen) = open.partition(n=> n.parents.foldLeft(true)(_&& deps.contains(_)))
        // create new nodes and add those to the depedencies
        val newDeps = deps ++ current.map(n=> (n.label -> new Node(n.label, n.parents.map(p=>deps(p)), n.model)))
        createNodesWithDeps(newDeps, newOpen)
      }
    }

    val nodes = createNodesWithDeps(HashMap(), tmpNodes)

    new DirectedAcyclicGraph(nodes.asInstanceOf[HashMap[String, Node]])
  }
}
