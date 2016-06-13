package de.oweissbarth.model

import de.oweissbarth.graph.Node
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.regression.LinearRegression

class SimpleLinearModelProvider extends ModelProvider {
  def getModel(d: DataFrame, parents : Array[Node]): SimpleLinearModel = {

    val lr = new LinearRegression()
    val result = lr.fit(d)


    return new SimpleLinearModel(result.coefficients, result.intercept)
  }
}