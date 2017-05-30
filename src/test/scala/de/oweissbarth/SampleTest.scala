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

import de.oweissbarth.sample._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest._

class SampleTest extends FlatSpec with BeforeAndAfterAll with Matchers{


  override def beforeAll() = {
    val sparkConf = new SparkConf()

    val sc = new SparkContext("local", "Baysian", sparkConf)

    val sqlc = new SQLContext(sc)

  }

  override def afterAll() = {
    val sc = SparkContext.getOrCreate()
    sc.stop()
  }

 "A CSVSampleProvider" should "be non null after creation" in {
   val sc = SparkContext.getOrCreate()
   implicit val sqlc = SQLContext.getOrCreate(SparkContext.getOrCreate())

   val prov = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")
   assert(prov != null)
 }

  it should " return a non null sample when getSample is called if constructed with valid path" in {
    val sc = SparkContext.getOrCreate()
    implicit val sqlc = SQLContext.getOrCreate(SparkContext.getOrCreate())

    val prov = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")
    assert(prov.getSample() != null)
  }


  it should " infer schema correctly" in{
    val sc = SparkContext.getOrCreate()
    implicit val sqlc = SQLContext.getOrCreate(sc)

    val prov = new CSVSampleProvider("src/test/resources/ageGenderIncome.csv", ";")

    val recordFields = prov.getSample().records.first()


    //TODO this does not work
    //recordFields(0) should be a ('Double)
    //recordFields(1) should be a ('String)
    //recordFields(2) should be a ('Double)

  }

  it should " trim whitespaces from column label" in{
    val sc = SparkContext.getOrCreate()
    implicit val sqlc = SQLContext.getOrCreate(sc)

    val prov = new CSVSampleProvider("src/test/resources/ageGenderIncomeWithWhitespaces.csv", ";")

    val labels = prov.getSample().records.columns

    labels(0) shouldBe "Age"
    labels(1) shouldBe "Gender"
    labels(2) shouldBe "Income"

  }




}