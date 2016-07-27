package de.oweissbarth

import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.synthesis.SimpleSynthesizer
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
