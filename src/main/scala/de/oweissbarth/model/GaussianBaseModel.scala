package de.oweissbarth.model
import org.apache.spark.sql.DataFrame

/**
  * Created by oliver on 6/13/16.
  */
class GaussianBaseModel(val expectation: Double, val variance: Double) extends Model{
  override def model(dependencies: DataFrame) = {

  }
}
