package de.oweissbarth.graph

import org.apache.spark.sql.SQLContext

import scala.xml.XML

class GraphMLGraphProvider(filepath: String) extends GraphProvider{

  
  
	def getGraph(): DirectedAcyclicGraph = {
    val graph = new DirectedAcyclicGraph()

    val xml = XML.loadFile(filepath)
    val labels = (xml \\ "node" \\ "@id").map(_.text).toList
    val edges = (xml \\ "edge" ).map({case(a)=>(a.attribute("source").getOrElse("").toString, a.attribute("target").getOrElse("").toString)})

    val edgeIds = edges.map({case(s, t)=> (labels.indexOf(s), labels.indexOf(t))}).toList

    graph.fromLabelsAndEdges(labels, edgeIds)
    graph
	}
}