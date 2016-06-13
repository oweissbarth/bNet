package de.oweissbarth.model

import de.oweissbarth.graph.Node
import org.apache.spark.sql.DataFrame


abstract class Model {
  def model(dependencies:  DataFrame)
}