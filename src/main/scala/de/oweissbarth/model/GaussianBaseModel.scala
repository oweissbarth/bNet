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
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions.{randn, udf}



/** hold the parameters of the simple 2 dimensional gaussian model
  *
  * @constructor creates a new gaussian model
  * @param expectation is turning point of the gaussian
  * @param variance is the variance of the gaussian
  */
case class GaussianBaseModel(expectation: Double, variance: Double) extends IntervalModel{
  override def model(dependencies: DataFrame, node: Node, count: Long): DataFrame = {
    val sc = SparkContext.getOrCreate()
    val sqlc = new SQLContext(sc)

    val modelApply = udf((d: Double) => d*Math.sqrt(variance)+expectation)

    dependencies.withColumn(node.label, modelApply(randn()))

  }

}

object GaussianBaseModel extends Persist[GaussianBaseModel]{
  /** creates a new GaussianBaseModel from json
    *
    */
  def fromJson(json: String): GaussianBaseModel = {
    /*val logger = LogManager.getLogger("GaussianBaseModel from Json")*/

    implicit val formats = DefaultFormats


    parse(json).extract[GaussianBaseModel]
  }

}
