package de.oweissbarth.model
import de.oweissbarth.graph.Node
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, variance}

/**
  * Created by oliver on 6/13/16.
  */
class GaussianBaseModelProvider extends IntervalModelProvider{
  override def getModel(d: DataFrame, parents: Array[Node]): GaussianBaseModel ={
    val mean = d.select(avg(d.columns(0))).first().getDouble(0)
    val vari = d.select(variance(d.columns(0))).first().getDouble(0)
    new GaussianBaseModel(mean, vari)
  }
}
