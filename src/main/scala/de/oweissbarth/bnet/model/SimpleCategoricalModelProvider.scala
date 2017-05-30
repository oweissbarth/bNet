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

package de.oweissbarth.bnet.model
import de.oweissbarth.bnet.graph.Node
import org.apache.spark.sql.DataFrame


/** supplies a SimpleCategoricalModel
  *
  */
class SimpleCategoricalModelProvider extends CategoricalModelProvider{
  override def getModel(d: DataFrame, parents: Array[Node]): SimpleCategoricalModel = {

    val total: Double = d.count()

    val distribution = d.groupBy(d.columns(0)).count().collect().map(r => (r.get(0).toString -> r.getLong(1)/total)).toMap
    new SimpleCategoricalModel(distribution)
  }
}
