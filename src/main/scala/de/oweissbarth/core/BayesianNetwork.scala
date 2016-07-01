package de.oweissbarth.core

import de.oweissbarth.graph._
import de.oweissbarth.sample._
import de.oweissbarth.model._
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/** The full bayesian network
  *
  * When initialized this constructs a new spark context.
  *
  * @todo In the future it should be possible to pass an existing spark context as a parameter
  *
  * @constructor creates a new bayesian network
  *
  * @param graphProvider supplies a graph to the network
  */
class BayesianNetwork(private val graphProvider: GraphProvider){

  val sparkConf = new SparkConf()

  implicit val sc = new SparkContext("local", "Baysian", sparkConf)

  implicit val sqlc = new SQLContext(sc)


  val logger = LogManager.getRootLogger
  
  logger.info("Constructing Bayesian Network...")
  logger.info("Getting graph...")
  private val graph = graphProvider.getGraph()
  logger.info("done...")
  logger.info("done...")

  /*logger.info("trying to match columns and nodes...")
  logger.info(sample.records.schema)

  { r => if(graph.nodes.contains(r.label)){
                                    logger.info("matched column "+d.label+" to node with same name")
                                    graph.nodes(d.label).dataSet = Some(d)
                                  }else{ 
                                    logger.warn("Could not match column "+d.label+" to a node")}
                          }*/
  logger.info("Graph built.")

  /** Models all nodes in the bayesian network
    *
    * @note Modelproviders for each node need to be specfied before running fit.
    *
    * @param sp supplies the data sample to model against
    */
  def fit(sp: SampleProvider):Unit = {
    graph.nodes.values.map(_.fit(sp.getSample().records))
  }

  /** specifies how to model a specific node
    *
    * @param label the node's label
    * @param modelType describes how a node is modelled
    */
  def setModelType(label:String, modelType: ModelProvider) ={
    graph.nodes(label).modelProvider = Some(modelType)
  }

  /** returns the model type for a specific node
    *
    * @param label the node's label
    * @return the nodes model provider
    */
  def getModelType(label:String):ModelProvider ={
    graph.getNodeByLabel(label).modelProvider.get
  }

  def getModel(label :String) = {
    graph.getNodeByLabel(label).model.get.asJson()
  }

  /** ends the bayesian network and stops the spark context
    *
    */
  def close() = {
    sc.stop()
  }


}