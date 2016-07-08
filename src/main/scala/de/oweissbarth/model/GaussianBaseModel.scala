package de.oweissbarth.model

import org.apache.spark.sql.DataFrame
import org.apache.log4j.LogManager
import org.json4s.jackson.JsonMethods._


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
    s"""{"GaussianBaseModel": {"expectation": $expectation, "variance": $variance}}"""
  }
}

object GaussianBaseModel extends Persist[GaussianBaseModel]{
  /** creates a new GaussianBaseModel from json
    *
    */
  def fromJson(json: String): GaussianBaseModel = {
    val logger = LogManager.getLogger("GaussianBaseModel from Json")

    val ast = parse(json)
    if(ast.children.length != 1)
      logger.warn(s"$json should only contain one model!")
    val values = ast.children(0).values.asInstanceOf[Map[String, Double]]

    new GaussianBaseModel(values("expectation"), values("variance"))
  }

}
