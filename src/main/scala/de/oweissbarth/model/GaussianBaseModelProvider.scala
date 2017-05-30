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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, variance}

/** supplies a GaussianModel to a bayesian network
  * @constructor creates a new model supplier
  */
class GaussianBaseModelProvider extends IntervalModelProvider{
  override def getModel(d: DataFrame, parents: Array[Node]): GaussianBaseModel ={
    val mean = d.select(avg(d.columns(0))).first().getDouble(0)
    val vari = d.select(variance(d.columns(0))).first().getDouble(0)
    new GaussianBaseModel(mean, vari)
  }
}
