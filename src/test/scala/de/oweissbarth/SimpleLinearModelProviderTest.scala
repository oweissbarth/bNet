package de.oweissbarth

import de.oweissbarth.graph.Node
import de.oweissbarth.model.{GaussianBaseModelProvider, SimpleCategoricalModelProvider, SimpleLinearModelProvider}
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

  it should " compute the correct linear parameters within a given dataset without categorical parents " in {

    val sp = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")

    val scmp = new SimpleLinearModelProvider()

    val subDataSet = sp.getSample().records.select("y", "x")

    val model = scmp.getModel(subDataSet, Array(new Node("x")))

    model.parameters.get("").get._1(0) should be (12.0 +- 0.5)
    model.parameters.get("").get._2 should be (300.0 +- 1.0)

  }

  it should " compute the correct linear parameters within a given dataset with categorical parents " in {

    val sp = new CSVSampleProvider("src/test/resources/ageGenderIncome.csv", ";")

    val scmp = new SimpleLinearModelProvider()

    val subDataSet = sp.getSample().records.select("Income", "Gender", "Age")

    val age = new Node("Age")
    val gender = new Node("Gender")

    age.modelProvider = Some(new GaussianBaseModelProvider())
    gender.modelProvider = Some(new SimpleCategoricalModelProvider())

    val model = scmp.getModel(subDataSet, Array(gender, age))
    
    model.parameters.get("M").get._1(0) should be (141.81 +- 0.1)
    model.parameters.get("M").get._2 should be (-1857.98 +- 0.1) // TODO this actually be 1927,...

  }
}
