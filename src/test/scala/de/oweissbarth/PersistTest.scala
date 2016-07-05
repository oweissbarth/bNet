package de.oweissbarth

import de.oweissbarth.model.{GaussianBaseModel, SimpleCategoricalModel, SimpleLinearModel}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._
import org.apache.spark.mllib.linalg.Vectors



class PersistTest extends FlatSpec with BeforeAndAfterAll with Matchers{

    override def beforeAll() = {
      val sparkConf = new SparkConf()

      val sc = new SparkContext("local", "Baysian", sparkConf)

      val sqlc = new SQLContext(sc)

    }

    override def afterAll() = {
      val sc = SparkContext.getOrCreate()
      sc.stop()
    }
    
    "A GaussianBaseModel " should "serialize to json correctly" in {
      val gm = new GaussianBaseModel(3.5, 1.34)
      gm.asJson() should be ("{GaussianBaseModel: {expectation: 3.5, variance: 1.34}}")
    }

    "A SimpleCategoricalModel " should "serialize to json correclty" in {
      val cm = new SimpleCategoricalModel(Map("M"->0.4, "F"->0.6))
      println(cm.asJson())
      cm.asJson() should be ("{SimpleCategoricalModel: {distribution: [M: 0.4, F: 0.6]}}")
    }

    "A SimpleLinearModel " should "serialize to json correctly" in{
      val lm = new SimpleLinearModel(Map("M"->(Vectors.dense(0.3, 0.1), 1.2)))
      lm.asJson() should be ("{SimpleLinearModel: {parameters: [{M: {gradient: [0.3,0.1], intercept: 1.2}}]}}")
    }

}