package de.oweissbarth.model
import org.apache.spark.sql.DataFrame

/** hold the parameters of the simple 2 dimensional gaussian model
  *
  * @constructor creates a new gaussian model
  *
  * @param expectation is turning point of the gaussian
  * @param variance is the variance of the gaussian
  */
class GaussianBaseModel(val expectation: Double, val variance: Double) extends Model{
  override def model(dependencies: DataFrame) = {

  }

  /** returns a json representation of the model
    *
    * @return a json representation of the model
    */
  override  def asJson() = {
    s"{GaussianBaseModel: {expectation: $expectation, variance: $variance}}"
  }
}
