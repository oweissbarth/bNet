package de.oweissbarth

import de.oweissbarth.graph.Node
import de.oweissbarth.model.SimpleLinearModelProvider
import de.oweissbarth.sample.CSVSampleProvider
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by oliver on 6/11/16.
  */
class SimpleLinearModelProviderTest extends  FlatSpec with Matchers with BeforeAndAfterAll{
  override def beforeAll() = {
    val sparkConf = new SparkConf()

    val sc = new SparkContext("local", "Baysian", sparkConf)

    val sqlc = new SQLContext(sc)

  }

  override def afterAll() = {
    val sc = SparkContext.getOrCreate()
    sc.stop()
  }

  "A SimpleLinearModelProvider" should "initialize without errors" in {
    val scmp = new SimpleLinearModelProvider()


    scmp should not be null
  }

  it should " compute the correct distribution of categories within a given dataset" in {

    val sp = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")

    val scmp = new SimpleLinearModelProvider()

    val subDataSet = sp.getSample().records.select("y", "x")

    val model = scmp.getModel(subDataSet, Array(new Node("x")))

    model.parameters.get("").get._1(0).toInt should be (12)
    model.parameters.get("").get._2.toInt should be (300)

  }
}
