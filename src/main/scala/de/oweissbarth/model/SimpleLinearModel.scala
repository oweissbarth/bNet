package de.oweissbarth.model
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.Vector


class SimpleLinearModel(val parameters: Map[String, (Vector, Double)]) extends Model{
  override def model(dependencies: DataFrame): Unit = {}

  override  def asJson() = {
    s"{parameters: [${parameters}]}"
  }
}