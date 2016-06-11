package de.oweissbarth.graph

import org.apache.spark.sql.SQLContext

abstract class GraphProvider{
	def getGraph():DirectedAcyclicGraph;
}
