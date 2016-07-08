package de.oweissbarth

import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.graph.{GraphMLGraphProvider, Node}
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
      gm.asJson() should be ("""{"GaussianBaseModel": {"expectation": 3.5, "variance": 1.34}}""")
    }

    it should " deserialize from json correctly" in {
      val gm = GaussianBaseModel.fromJson("""{"GaussianBaseModel": {"expectation": 3.5, "variance": 1.34}}""")
      gm.expectation should be (3.5)
      gm.variance should be (1.34)
    }

    "A SimpleCategoricalModel " should "serialize to json correctly" in {
      val cm = new SimpleCategoricalModel(Map("M"->0.4, "F"->0.6))
      cm.asJson() should be ("""{"SimpleCategoricalModel": {"distribution": {"M": 0.4, "F": 0.6}}}""")
    }

    it should "deserialize from json correctly" in{
      val cm = SimpleCategoricalModel.fromJson("""{"SimpleCategoricalModel": {"distribution": {"M": 0.4, "F": 0.6}}}""")
      cm.distribution("M") should be (0.4)
      cm.distribution("F") should be (0.6)
    }

    "A SimpleLinearModel " should "serialize to json correctly" in{
      val lm = new SimpleLinearModel(Map("M"->(Vectors.dense(0.3, 0.1), 1.2)))
      lm.asJson() should be ("""{"SimpleLinearModel": {"parameters": {"M": {"gradient": [0.3,0.1], "intercept": 1.2}}}}""")
    }

    it should "deserialize from json correctly" in {
      val lm = SimpleLinearModel.fromJson("""{"SimpleLinearModel": {"parameters": {"M": {"gradient": [0.3,0.1], "intercept": 1.2}}}}""")

      lm.parameters("M")._1 should be (Vectors.dense(0.3,0.1))
      lm.parameters("M")._2 should be (1.2)
    }

   "A Node " should "serialize to json correctly" in {
     val cm = new SimpleCategoricalModel(Map("M"->0.4, "F"->0.6))
     val n = new Node("Income", Array(new Node("Age"), new Node("Gender")))
     n.model = Some(cm)
     n.asJson() should be ("{label: Income, parents: [Age, Gender], model: {SimpleCategoricalModel: {distribution: [M: 0.4, F: 0.6]}}}")
     n.model = None
     n.asJson() should be ("{label: Income, parents: [Age, Gender], model: None}")

   }

    "A Directed Acyclic Graph should" should "serialize to json correctly" in {
      val gp = new GraphMLGraphProvider("src/test/resources/ageGenderIncome.gml")
      gp.getGraph.asJson should be ("{BayesianNetwork: [{label: Age, parents: [], model: None}, {label: Gender, parents: [], model: None}, {label: Income, parents: [Age, Gender], model: None}]}")

    }

}