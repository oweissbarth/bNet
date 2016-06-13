package de.oweissbarth.model

import de.oweissbarth.graph.Node
import org.apache.spark.sql.DataFrame


abstract class ModelProvider() {
  def getModel(d: DataFrame, parents: Array[Node]): Model

}