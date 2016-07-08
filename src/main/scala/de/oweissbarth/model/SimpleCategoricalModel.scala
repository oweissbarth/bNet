package de.oweissbarth.model
import de.oweissbarth.graph.Node
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.json4s.jackson.JsonMethods._

/** a categorical model with no dependencies
  *
  * @constructor  creates a simple catgorical model
  * @param distribution the propability of each category
  */
class SimpleCategoricalModel(val distribution: Map[String, Double]) extends Model {
  override def model(dependencies: DataFrame): Unit = {}


  /** returns a human readable representation of the model
    *
    * @return a human readable representation of the model
    */
  override def toString(): String ={
    "SimpleCategoricalModel: <"+ distribution.foreach(p => p.toString)+">"
  }

  /** returns  a json representation of the model
    *
    * @return a json representation of the model
    */
  override  def asJson() = {
    s"""{"SimpleCategoricalModel": {"distribution": {${distribution.map(e => s""""${e._1}": ${e._2}""").reduce(_+", "+_)}}}}"""
  }
}

object SimpleCategoricalModel extends Persist[SimpleCategoricalModel] {
  /** creates a new Model from json
    *
    */
  override def fromJson(json: String): SimpleCategoricalModel = {
    val logger = LogManager.getLogger("SimpleCategoricalModel from Json")

    val ast = parse(json)
    if (ast.children.length != 1)
      logger.warn(s"$json should only contain one model!")
    val values = ast.children(0).children(0).values.asInstanceOf[Map[String, Double]]

    new SimpleCategoricalModel(values)
  }
}
