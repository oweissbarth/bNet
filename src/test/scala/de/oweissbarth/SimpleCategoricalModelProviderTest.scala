package de.oweissbarth

import de.oweissbarth.model.SimpleCategoricalModelProvider
import de.oweissbarth.sample.CSVSampleProvider
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.math.round

/**
  * Created by oliver on 6/11/16.
  */
class SimpleCategoricalModelProviderTest extends  FlatSpec with Matchers with BeforeAndAfterAll{
  override def beforeAll() = {
    val sparkConf = new SparkConf()

    val sc = new SparkContext("local", "Baysian", sparkConf)

    val sqlc = new SQLContext(sc)

  }

  override def afterAll() = {
    val sc = SparkContext.getOrCreate()
    sc.stop()
  }

  "A SimpleCategoricalModelProvider" should "initialize without errors" in {
    val scmp = new SimpleCategoricalModelProvider()


    scmp should not be null
  }

  it should " compute the correct distribution of categories within a given dataset" in {
    val sc = SparkContext.getOrCreate()
    implicit val sqlc = SQLContext.getOrCreate(SparkContext.getOrCreate())


    val sp = new CSVSampleProvider("src/test/resources/ageGenderIncome.csv", ";")

    val scmp = new SimpleCategoricalModelProvider()


    val model = scmp.getModel(sp.getSample().records.select("Gender"), Array())

    print(round(model.distribution(0)*10))

    round(model.distribution(0)*10) should be (4)
    round(model.distribution(1)*10) should be (6)
  }
}
