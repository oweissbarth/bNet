package de.oweissbarth.graph

import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.graph.DirectedAcyclicGraph.{UnlinkedNode, buildNodes}
import de.oweissbarth.model.{Model, ModelProvider}
import org.json4s.{DefaultFormats, ShortTypeHints}
import org.json4s.JsonAST.JValue

import scala.collection.immutable.HashMap

/** a graph with directed edges and without cycles
  *
  * @constructor creates a new empty graph
  */
class DirectedAcyclicGraph(val nodes: HashMap[String, Node]= HashMap()){

  /** returns the node with a given label
    *
    * @param label the node's label
    * @return the node with the given label
    */
	def getNodeByLabel(label: String): Node = {
	  nodes(label)
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
    val unlinkedNodes = nodes.values.map(n=>UnlinkedNode(n.label, n.parents.map(_.label).toList, n.model.getOrElse(null), n.modelProvider.getOrElse(null))).toList

    new DirectedAcyclicGraph(buildNodes(unlinkedNodes).asInstanceOf[HashMap[String, Node]])
  }
}

object DirectedAcyclicGraph{
  /** creates a new DirectedAcyclicGraph from a json ast
    *
    * @param ast the json ast as created by json4s
    * @return a full directed acyclic graph
    */
  def fromJson(ast: JValue): DirectedAcyclicGraph = {
    implicit  val formats = new DefaultFormats{
      override val typeHints = ShortTypeHints(BayesianNetwork.modelTypes)
      override val typeHintFieldName = "type"
    }

    val tmpNodes = for(c <- ast.children)yield  c.extract[UnlinkedNode]

    val sorted = topologicSort(tmpNodes)

    if(!sorted.isDefined) throw new GraphIsCyclicException

    val nodes = buildNodes(sorted.get)


    new DirectedAcyclicGraph(nodes.asInstanceOf[HashMap[String, Node]])
  }


  /** creates a new DirectedAcyclicGraph from a list of labels and edges
    *
    * throws a GraphIsCyclicException if the constructed graph is cyclic
    *
    * @param labels a list of node labels
    * @param edges a list of tuples representing the edges (zero indexed)
    * @return a full directed acyclic graph
    */
  def fromLabelsAndEdges(labels: List[String], edges: List[(Int, Int)]): DirectedAcyclicGraph = {

    val withOutEdges = labels.map(UnlinkedNode(_, List(), null))

    val groupedEdges = edges.groupBy(_._2)map{case (labelID: Int, edges: List[(Int, Int)]) => (labels(labelID), edges.map(e=>labels(e._1)))}

    val unlinkedNodes = withOutEdges.map(n => n.copy(parents=groupedEdges.getOrElse(n.label, List())))

    val sorted = topologicSort(unlinkedNodes)

    if(!sorted.isDefined) throw new GraphIsCyclicException

    val nodes = buildNodes(sorted.get)


    new DirectedAcyclicGraph(nodes.asInstanceOf[HashMap[String, Node]])
  }


  /** Builds a Hashmap for construction of dag
    *
    * @param sorted the unlinked nodes in topological order
    * @return a hashmap from node label to linked node
    */
  private def buildNodes(sorted: List[UnlinkedNode]): Map[String, Node] = {
    var nodes = HashMap[String, Node]()
    sorted.foreach(n=>
      nodes += (n.label -> new Node(n.label, n.parents.map(nodes(_)).toArray, n.model, n.modelProvider))
    )
    nodes
  }


  /** sorts the given nodes topologically and checks if the graph is acyclic using kahn's algorithm
    *
    * models are preserved.
    *
    * @param a list of unlinked nodes
    * @return an option containing a sorted list of unlinked nodes
    */
  private def topologicSort(list: List[UnlinkedNode]):Option[List[UnlinkedNode]] = {
    var nodes = list
    var sorted : List[UnlinkedNode]= List()
    var sink = nodes.filter(_.parents.isEmpty)
    while(!sink.isEmpty) {
      var n = sink.head
      sink = sink.patch(0, Nil, 1)
      sorted = sorted :+ n
      nodes.filter(_.parents.contains(n.label)).foreach( m=> {
        val newM = m.copy(parents = m.parents.patch(m.parents.indexOf(n.label), Nil, 1))
        nodes = nodes.patch(nodes.indexOf(m), Seq(newM), 1)
        if(newM.parents.isEmpty){
          sink ::= newM
        }

      })
    }
    if(nodes.foldLeft(true)(_ && _.parents.isEmpty)){
      Option(sorted.map(o => list.find(n=>n.label == o.label).get))
    }else{
      None
    }
  }

  private case class UnlinkedNode(label: String, parents: List[String], model: Model, modelProvider: ModelProvider = null)

  case class GraphIsCyclicException() extends Exception

}
