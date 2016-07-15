package de.oweissbarth.graph

import scala.xml.XML

/** Reads a graph from a GraphML file. Supplies a graph  to the bayesian network
  * @constructor creates a new supplier
  * @param filepath the graphml file location
  */
class GraphMLGraphProvider(filepath: String) extends GraphProvider{


  /** returns the graph as read from the graphml file
    *
    * @return a directed acyclic graph
    */
	def getGraph(): DirectedAcyclicGraph = {
    val xml = XML.loadFile(filepath)
    val labels = (xml \\ "node" \\ "@id").map(_.text).toList
    val edges = (xml \\ "edge" ).map({case(a)=>(a.attribute("source").getOrElse("").toString, a.attribute("target").getOrElse("").toString)})

    val edgeIds = edges.map({case(s, t)=> (labels.indexOf(s), labels.indexOf(t))}).toList

    DirectedAcyclicGraph.fromLabelsAndEdges(labels, edgeIds)
	}
}