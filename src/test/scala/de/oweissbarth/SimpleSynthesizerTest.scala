package de.oweissbarth

import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.synthesis.SimpleSynthesizer
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.apache.spark.sql.functions.{avg, variance}


import scala.io.Source


class SimpleSynthesizerTest extends  FlatSpec with Matchers with BeforeAndAfterAll {

  "A SimpleSynthesizer" should "work" in {
    val json = Source.fromFile("src/test/resources/ageGenderIncome.json").getLines().reduce(_+_)
    val bn = BayesianNetwork.fromJson(json)

    val syn = new SimpleSynthesizer(bn)


    val count = 5000

    val result = syn.synthesize(count)

    result.count() should be (count)

    val selectionAge = result.select(avg("age"), variance("age")).first()

    selectionAge.getDouble(0) should be (48.24 +- 1.5)

    selectionAge.getDouble(1) should be (417.23 +- 15.0)

    val selectionGender = result.groupBy("gender").count()

    selectionGender.where("gender = 'F'").first().getLong(1)/count.toDouble should be (0.393 +- 0.05)

    selectionGender.where("gender = 'M'").first().getLong(1)/count.toDouble should be (0.607 +- 0.05)

    bn.close()


  }

}
