package de.oweissbarth.model
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.Vector


class SimpleLinearModel(val coefficients: Vector, val intercept: Double) extends Model{
  override def model(dependencies: DataFrame): Unit = {}
}