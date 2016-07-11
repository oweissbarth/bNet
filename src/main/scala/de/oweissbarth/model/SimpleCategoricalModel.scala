package de.oweissbarth.model
import org.apache.spark.sql.DataFrame
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._
import org.json4s.jackson.Serialization
import org.json4s._

/** a categorical model with no dependencies
  *
  * @constructor  creates a simple catgorical model
  * @param distribution the propability of each category
  */
case class SimpleCategoricalModel(distribution: Map[String, Double]) extends Model {
  override def model(dependencies: DataFrame): Unit = {}


  /** returns a human readable representation of the model
    *
    * @return a human readable representation of the model
    */
  override def toString(): String = {
    "SimpleCategoricalModel: <" + distribution.foreach(p => p.toString) + ">"
  }

}

object SimpleCategoricalModel extends Persist[SimpleCategoricalModel] {
  /** creates a new Model from json
    *
    */
  override def fromJson(json: String): SimpleCategoricalModel = {
   /* val logger = LogManager.getLogger("SimpleCategoricalModel from Json")*/
   implicit val formats = DefaultFormats

    parse(json).extract[SimpleCategoricalModel]
  }
}
