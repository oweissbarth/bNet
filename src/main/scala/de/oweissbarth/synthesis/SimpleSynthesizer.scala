/*
 * Copyright 2017 Oliver Weissbarth
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
