package de.oweissbarth.model

import org.apache.spark.sql.DataFrame
import org.apache.log4j.LogManager
import org.json4s.{DefaultFormats, ShortTypeHints}
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._


/** hold the parameters of the simple 2 dimensional gaussian model
  *
  * @constructor creates a new gaussian model
  *
  * @param expectation is turning point of the gaussian
  * @param variance is the variance of the gaussian
  */
case class GaussianBaseModel(expectation: Double, variance: Double) extends Model{
  override def model(dependencies: DataFrame) = {

  }

  /** returns a json representation of the model
    *
    * @return a json representation of the model
    */
  override  def asJson() = {
    implicit  val formats = new DefaultFormats{
      override val typeHints = ShortTypeHints(List(classOf[SimpleLinearModel], classOf[SimpleCategoricalModel], classOf[GaussianBaseModel]))
      override val typeHintFieldName = "type"
    }
    write(this)
  }
}

object GaussianBaseModel extends Persist[GaussianBaseModel]{
  /** creates a new GaussianBaseModel from json
    *
    */
  def fromJson(json: String): GaussianBaseModel = {
    /*val logger = LogManager.getLogger("GaussianBaseModel from Json")*/

    implicit val formats = DefaultFormats


    parse(json).extract[GaussianBaseModel]
  }

}
