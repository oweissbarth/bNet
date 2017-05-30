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

import de.oweissbarth.bnet.model.{GaussianBaseModelProvider, SimpleLinearModelProvider}
import de.oweissbarth.bnet.sample.CSVSampleProvider
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by oliver on 6/11/16.
  */
class GaussianBaseModelProviderTest extends  FlatSpec with Matchers with BeforeAndAfterAll{
  override def beforeAll() = {
    val sparkConf = new SparkConf()

    val sc = new SparkContext("local", "Baysian", sparkConf)

    val sqlc = new SQLContext(sc)

  }

  override def afterAll() = {
    val sc = SparkContext.getOrCreate()
    sc.stop()
  }

  "A GaussianBaseModelProvider" should "initialize without errors" in {
    val scmp = new GaussianBaseModelProvider()


    scmp should not be null
  }

  it should " compute the correct exspectation and variance values " in {
    val sc = SparkContext.getOrCreate()
    implicit val sqlc = SQLContext.getOrCreate(SparkContext.getOrCreate())


    val sp = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")

    val gbmp = new GaussianBaseModelProvider()

    val data = sp.getSample().records

    val model = gbmp.getModel(data, Array())

    model.expectation should be (60.0 +- 0.2)
    model.variance should be (550.0 +- 1.0)

  }
}
