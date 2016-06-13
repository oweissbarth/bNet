package de.oweissbarth.model
import de.oweissbarth.graph.Node
import org.apache.spark.sql.DataFrame


class SimpleCategoricalModelProvider extends CategoricalModelProvider{
  override def getModel(d: DataFrame, parents: Array[Node]): SimpleCategoricalModel = {

    val total: Double = d.count()

    val amount = d.groupBy(d.columns(0)).count().map(a => a.getLong(1)/total)

    new SimpleCategoricalModel(amount.collect())
  }
}
