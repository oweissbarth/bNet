package de.oweissbarth.model

import de.oweissbarth.graph.Node
import org.apache.spark.sql.DataFrame

/** supplies a model to a bayesian network
  *
  */
abstract class ModelProvider{
  /** computes a model based on sample data and the parent nodes
    *
    * @note The first column of d is always the column to model
    *
    * @param d the sample data
    * @param parents the nodes on which the computed model might depend
    * @return the calculated model
    */
  def getModel(d: DataFrame, parents: Array[Node]): Model

}