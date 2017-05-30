/*
 * Copyright 2017 Oliver Weissbarth
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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