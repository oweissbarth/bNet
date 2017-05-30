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

package de.oweissbarth

import de.oweissbarth.bnet.core.BayesianNetwork
import de.oweissbarth.bnet.synthesis.SimpleSynthesizer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.spark.sql.functions.{avg, variance}


import scala.io.Source


class SimpleSynthesizerTest extends  FlatSpec with Matchers with BeforeAndAfterAll {

  "A SimpleSynthesizer" should " synthesize data with the specified distributions" in {
    val json = Source.fromFile("src/test/resources/ageGenderIncome.json").getLines().reduce(_+_)
    val bn = BayesianNetwork.fromJson(json)

    val syn = new SimpleSynthesizer(bn)


    val count = 2500

    val result = syn.synthesize(count)

    result.count() should be (count)

    val selectionAge = result.select(avg("Age"), variance("Age")).first()

    selectionAge.getDouble(0) should be (48.24 +- 1.5)

    selectionAge.getDouble(1) should be (417.23 +- 20.0)

    val selectionGender = result.groupBy("Gender").count()

    selectionGender.where("Gender = 'F'").first().getLong(1)/count.toDouble should be (0.393 +- 0.05)

    selectionGender.where("Gender = 'M'").first().getLong(1)/count.toDouble should be (0.607 +- 0.05)

    bn.close()


  }

}
