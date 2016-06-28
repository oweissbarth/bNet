package de.oweissbarth.core

import de.oweissbarth.graph._
import de.oweissbarth.sample._
import de.oweissbarth.model._
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

//TODO add alternative argument order
class BayesianNetwork(private val graphProvider: GraphProvider, private val sampleProvider: SampleProvider){

  val sparkConf = new SparkConf()

  implicit val sc = new SparkContext("local", "Baysian", sparkConf)

  implicit val sqlc = new SQLContext(sc)


  val logger = LogManager.getRootLogger
  
  logger.info("Constructing Bayesian Network...")
  logger.info("Getting graph...")
  private val graph = graphProvider.getGraph()
  logger.info("done...")
  logger.info("Getting sample...")
  private val sample = sampleProvider.getSample()
  logger.info("done...")
  
  logger.info("trying to match columns and nodes...")
  logger.info(sample.records.schema)

  /*{ r => if(graph.nodes.contains(r.label)){
                                    logger.info("matched column "+d.label+" to node with same name")
                                    graph.nodes(d.label).dataSet = Some(d)
                                  }else{ 
                                    logger.warn("Could not match column "+d.label+" to a node")}
                          }*/
  logger.info("Graph built.")

  def fit():Unit = {
    graph.nodes.values.map(_.fit(sample.records))
  }
  
  def setModelType(label:String, modelType: ModelProvider) ={
    graph.nodes(label).modelProvider = Some(modelType)
  }
  
  def getModelType(label:String):ModelProvider ={
    graph.nodes(label).modelProvider.get
  }

  def setNodeType(label: String, nodeType: Int)= {
    graph.nodes(label).nodeType = nodeType
  }

  def getNodeType(label: String) = {
    graph.nodes(label).nodeType
  }

  def getModel(label :String) = {
    graph.nodes(label).model.get.asJson()
  }

  def close() = {
    sc.stop()
  }


}

object BayesianNetwork{
  val NONE        : Int = -1
  val CATEGORICAL : Int =  0
  val INTERVAL    : Int =  1


}