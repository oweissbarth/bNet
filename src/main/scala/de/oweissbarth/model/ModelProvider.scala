package de.oweissbarth.model

import org.apache.spark.sql.DataFrame


abstract class ModelProvider() {
  def getModel(d: DataFrame): Model

}