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

import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.graph.Node
import org.apache.spark.sql.DataFrame
import org.json4s.jackson.Serialization._
import org.json4s.{DefaultFormats, ShortTypeHints}

/** a model that holds all calaculated parameters
  *
  */
trait Model{
  /** applies the model to the given input parameters
    *
    * @param dependencies the variables of the mdel
    */
  def model(dependencies:  DataFrame, node: Node, count: Long): DataFrame

  /** returns a json representation of the model
    *
    * @return a json representation of the model
    */
  def asJson(): String = {
    implicit  val formats = new DefaultFormats{
      override val typeHints = ShortTypeHints(BayesianNetwork.modelTypes)
      override val typeHintFieldName = "type"
    }
    write(this)
  }
}

trait CategoricalModel extends Model
trait IntervalModel extends Model

abstract trait Persist[T /*<: Persist[T]*/]{ // TODO this is not working. No idea why
  /** creates a new Model from json
    *
    */
  def fromJson(json: String) : T

}