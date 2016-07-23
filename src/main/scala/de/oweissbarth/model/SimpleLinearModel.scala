package de.oweissbarth.model
import org.apache.spark.sql.DataFrame
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

/** a simple linear model that holds multiple hyperplane for different category combinations
  *
  * @param parameters the parameters of the linear model
  */
case class SimpleLinearModel(parameters: Map[String, (Array[Double], Double)]) extends Model{
  override def model(dependencies: DataFrame, count: Long): DataFrame = {

    dependencies
  }

}

object SimpleLinearModel extends Persist[SimpleLinearModel] {
  /** creates a new Model from json
    * @todo error handeling
    *
    * @param json the json input
    */
  override def fromJson(json: String): SimpleLinearModel = {
    /*val logger = LogManager.getLogger("SimpleLinearModel from Json")*/
    implicit val formats = Serialization.formats(ShortTypeHints(List(classOf[SimpleLinearModel])))
    parse(json).extract[SimpleLinearModel]
  }
}