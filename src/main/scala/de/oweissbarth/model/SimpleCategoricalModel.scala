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

package de.oweissbarth.model
import de.oweissbarth.graph.Node
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{rand, udf}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s._

/** a categorical model with no dependencies
  *
  * @constructor  creates a simple catgorical model
  * @param distribution the propability of each category
  */
case class SimpleCategoricalModel(distribution: Map[String, Double]) extends CategoricalModel {
  override def model(dependencies: DataFrame, node: Node, count : Long): DataFrame = {

    val sc = SparkContext.getOrCreate()
    val sqlc = new SQLContext(sc)


    def aggragateClasses(l: List[(String, Double)]): List[(Double, String)] = {
      var agg  = 0.0
      for(e <- l )
        yield ({agg += e._2; agg}, e._1)

    }

    val aggregated = aggragateClasses(distribution.toList)


    val modelApply = udf((d: Double)=> aggregated.filter(_._1>=d).head._2)



    dependencies.withColumn(node.label, modelApply(rand()))

  }


  /** returns a human readable representation of the model
    *
    * @return a human readable representation of the model
    */
  override def toString(): String = {
    "SimpleCategoricalModel: <" + distribution.map(p => p._1+": "+p._2).reduce(_+", "+_) + ">"
  }

}

object SimpleCategoricalModel extends Persist[SimpleCategoricalModel] {
  /** creates a new Model from json
    *
    */
  override def fromJson(json: String): SimpleCategoricalModel = {
   /* val logger = LogManager.getLogger("SimpleCategoricalModel from Json")*/
   implicit val formats = DefaultFormats

    parse(json).extract[SimpleCategoricalModel]
  }
}
