package de.oweissbarth.model
import de.oweissbarth.graph.Node
import org.apache.spark.sql.DataFrame


/** supplies a SimpleCategoricalModel
  *
  */
class SimpleCategoricalModelProvider extends CategoricalModelProvider{
  override def getModel(d: DataFrame, parents: Array[Node]): SimpleCategoricalModel = {

    val total: Double = d.count()

    val distribution = d.groupBy(d.columns(0)).count().collect().map(r => (r.getString(0) -> r.getLong(1)/total)).toMap
    new SimpleCategoricalModel(distribution)
  }
}
