package de.oweissbarth.graph

/** supplies the bayesian network with a graph
  *
  */
abstract class GraphProvider{
  /** returns a directed acyclic graph
    *
    * @return a directed acyclic graph
    */
  def getGraph():DirectedAcyclicGraph;
}
