package de.oweissbarth.model
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.Vector

/** a simple linear model that holds multiple hyperplane for different category combinations
  *
  * @param parameters the parameters of the linear model
  */
class SimpleLinearModel(val parameters: Map[String, (Vector, Double)]) extends Model{
  override def model(dependencies: DataFrame): Unit = {}

  override  def asJson() = {
    s"{parameters: [${parameters}]}"
  }
}