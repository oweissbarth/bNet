package de.oweissbarth.model
import de.oweissbarth.graph.Node
import org.apache.spark.sql.DataFrame

/**
  * Created by oliver on 6/11/16.
  */
class SimpleCategoricalModel(val distribution: Array[Double]) extends Model {
  override def model(dependencies: DataFrame): Unit = {}

  override def toString(): String ={
    "SimpleCategoricalModel: <"+ distribution.foreach(p => p.toString)+">"
  }

  override  def asJson() = {
    s"{distribution: ${distribution.map(_.toString).reduce(_+","+_)}}"
  }
}
