package de.oweissbarth

import de.oweissbarth.model.{GaussianBaseModelProvider, SimpleLinearModelProvider}
import de.oweissbarth.sample.CSVSampleProvider
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

    model.expectation.toInt should be (59)
    model.variance.toInt should be (550)

  }
}
