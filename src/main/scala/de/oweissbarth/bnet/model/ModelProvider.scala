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

/** supplies a model to a bayesian network
  *
  */
abstract class ModelProvider{
  /** computes a model based on sample data and the parent nodes
    *
    * @note The first column of d is always the column to model
    *
    * @param d the sample data
    * @param parents the nodes on which the computed model might depend
    * @return the calculated model
    */
  def getModel(d: DataFrame, parents: Array[Node]): Model

}