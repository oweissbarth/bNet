package de.oweissbarth.graph

import org.apache.spark.sql.SQLContext

abstract class GraphProvider{
	def getGraph()(implicit sqlc: SQLContext ):DirectedAcyclicGraph;
}
