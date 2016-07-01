package de.oweissbarth.model
import de.oweissbarth.graph.Node
import org.apache.spark.sql.DataFrame

/** a categorical model with no dependencies
  *
  * @constructor  creates a simple catgorical model
  *
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
    s"{distribution: ${distribution.map(_.toString).reduce(_+","+_)}}"
  }
}
