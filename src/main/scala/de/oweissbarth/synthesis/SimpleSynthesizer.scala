package de.oweissbarth.synthesis

import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.graph.Node
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}

class SimpleSynthesizer(bn: BayesianNetwork) {
  /** generates the specified number of sample based on models from the bayesianNetwork
    *
    * @param amount the number of samples to be generated
    */
  def synthesize(amount: Int) = {

      val sc = SparkContext.getOrCreate()
      val sqlc = new SQLContext(sc)

      bn.graph.nodes.values

    def traverse(nodes : List[Node], data: DataFrame):DataFrame = {
      nodes match{
        case Nil => data
        case x::xs => traverse(xs, x.model.get.model(data, x, amount))
      }
    }

    // create an intial column with specified length
    val seq = (0 until (amount))

    import sqlc.implicits._

    val initalColumn = sqlc.createDataset(seq)
      .toDF()
      .withColumnRenamed("value", bn.graph.nodes.values.head.label) // this colum will be replaced by the first column


    traverse(bn.graph.nodes.values.toList, initalColumn)
  }
}
