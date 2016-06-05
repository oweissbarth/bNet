package de.oweissbarth.core

import de.oweissbarth.graph._
import de.oweissbarth.sample._
import de.oweissbarth.model._
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}


class BayesianNetwork(private val graphProvider: GraphProvider, private val sampleProvider: SampleProvider) extends BayesianSparkContext{



  val logger = LogManager.getRootLogger
  
  logger.info("Constructing Bayesian Network...")
  logger.info("Getting graph...")
  private val graph = graphProvider.getGraph()
  logger.info("done...")
  logger.info("Getting sample...")
  private val sample = sampleProvider.getSample()
  logger.info("done...")
  
  logger.info("trying to match columns and nodes...")
  sample.dataSets.foreach { d => if(graph.nodes.contains(d.label)){
                                    logger.info("matched column "+d.label+" to node with same name")
                                    graph.nodes(d.label).dataSet = Some(d)
                                  }else{ 
                                    logger.warn("Could not match column "+d.label+" to a node")}
                          }
  logger.info("Graph built.")

  def fit():Unit = {
    graph.nodes.values.map(_.fit)
  }
  
  def setModelType(label:String, modelType: ModelProvider) ={
    graph.nodes(label).modelProvider = Some(modelType)
  }
  
  def getModelType(label:String):ModelProvider ={
    graph.nodes(label).modelProvider.get
  }

  def getColumnType(label: String): Int = {
    val set = graph.nodes(label).dataSet
    if(set == None){
      BayesianNetwork.NONE
    }else if(set.get.isInstanceOf[CategoricalDataSet]){
      BayesianNetwork.CATEGORICAL
    }else{
      BayesianNetwork.INTERVAL
    }
  }
}

object BayesianNetwork{
  val NONE : Int = -1
  val CATEGORICAL: Int = 0
  val INTERVAL : Int = 1


}

public trait BayesianSparkContext{
  val sparkConf = new SparkConf()

  val sc = new SparkContext("local", "Baysian", sparkConf)
}