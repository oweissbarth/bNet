package de.oweissbarth

import de.oweissbarth.core.BayesianNetwork
import de.oweissbarth.graph.{GraphMLGraphProvider, Node}
import de.oweissbarth.model.{SimpleCategoricalModelProvider, _}
import de.oweissbarth.sample.CSVSampleProvider
import org.apache.log4j.LogManager
import org.scalatest._



class PersistTest extends FlatSpec with BeforeAndAfterAll with Matchers{

    val wsReg = "\\s"
    
    "A GaussianBaseModel " should "serialize to json correctly" in {
      val gm = new GaussianBaseModel(3.5, 1.34)
      gm.asJson().replaceAll(wsReg, "") should be ("""{"type":"GaussianBaseModel","expectation":3.5,"variance":1.34}""")
    }

    it should " deserialize from json correctly" in {
      val gm = GaussianBaseModel.fromJson("""{"type":"GaussianBaseModel","expectation":3.5,"variance":1.34}""")
      gm.expectation should be (3.5)
      gm.variance should be (1.34)
    }

    "A SimpleCategoricalModel " should "serialize to json correctly" in {
      val cm = new SimpleCategoricalModel(Map("M"->0.4, "F"->0.6))
      cm.asJson().replaceAll(wsReg, "") should be ("""{"type":"SimpleCategoricalModel","distribution":{"M":0.4,"F":0.6}}""")
    }

    it should "deserialize from json correctly" in{
      val cm = SimpleCategoricalModel.fromJson("""{"type":"SimpleCategoricalModel","distribution":{"M":0.4,"F":0.6}}""")
      cm.distribution("M") should be (0.4)
      cm.distribution("F") should be (0.6)
    }

    "A SimpleLinearModel " should "serialize to json correctly" in{
      val lm = new SimpleLinearModel(Map("M"->(SimpleLinearModelParameterSet(Map(("Age"->0.3), ("Gender"->0.1)), 1.2))))
      lm.asJson().replaceAll(wsReg, "") should be ("""{"type":"SimpleLinearModel","parameters":{"M":{"type":"SimpleLinearModelParameterSet","slope":{"Age":0.3,"Gender":0.1},"intercept":1.2}}}""")
    }

    it should "deserialize from json correctly" in {
      val lm = SimpleLinearModel.fromJson("""{"type":"SimpleLinearModel","parameters":{"M":{"type":"SimpleLinearModelParameterSet","slope":{"Age":0.3,"Gender":0.1},"intercept":1.2}}}""")

      lm.parameters("M").slope should be (Map(("Age"->0.3), ("Gender"->0.1)))
      lm.parameters("M").intercept should be (1.2)
    }

   "A Node " should "serialize to json correctly" in {
     val cm = new SimpleCategoricalModel(Map("M"->0.4, "F"->0.6))
     val n = new Node("Income", Array(new Node("Age"), new Node("Gender")))
     n.model = Some(cm)
     n.asJson().replaceAll(wsReg, "") should be ("""{"label":"Income","parents":["Age","Gender"],"model":{"type":"SimpleCategoricalModel","distribution":{"M":0.4,"F":0.6}}}""")
     n.model = None
     n.asJson().replaceAll(wsReg, "") should be ("""{"label":"Income","parents":["Age","Gender"],"model":null}""")

   }

    it should "deserialize from json correctly" in {
      // NOTE shoud we support that?
      //val n = Node.fromJson("""{"label":"Income","parents":["Age","Gender"],"model":null}""")
    }

    "A Directed Acyclic Graph should" should "serialize to json correctly" in {
      val gp = new GraphMLGraphProvider("src/test/resources/ageGenderIncome.gml")
      gp.getGraph.asJson.replaceAll(wsReg, "") should be ("""{"BayesianNetwork":[{"label":"Age","parents":[],"model":null},{"label":"Gender","parents":[],"model":null},{"label":"Income","parents":["Age","Gender"],"model":null}]}""")

    }

    it should "deserialize from json correctly" in {
      val bn = BayesianNetwork.fromJson("""{"BayesianNetwork":[{"label":"Age","parents":[],"model":null},{"label":"Gender","parents":[],"model":null},{"label":"Income","parents":["Age","Gender"],"model":{"type":"SimpleCategoricalModel","distribution":{"M":0.4,"F":0.6}}}]}""")
      bn.close()
    }

    "A bayesian network" should "serialize to json and deserialize from it" in {
      val logger = LogManager.getLogger(s"Fitting BaysianNetwork")


      val sp = new CSVSampleProvider("src/test/resources/ageGenderIncome.csv", ";")

      val gp = new GraphMLGraphProvider("src/test/resources/ageGenderIncome.gml")

      val bn = new BayesianNetwork(gp)

      val slmp = new SimpleLinearModelProvider()
      val gbmp = new GaussianBaseModelProvider()
      val scmp = new SimpleCategoricalModelProvider()

      bn.setModelType("Age", gbmp)
      bn.setModelType("Gender", scmp)
      bn.setModelType("Income", slmp)

      bn.fit(sp)

      logger.warn(bn.getModel("Age"))
      logger.warn(bn.getModel("Gender"))
      logger.warn(bn.getModel("Income"))

      val json = bn.asJson()

      bn.close()

      val bn2 = BayesianNetwork.fromJson(json)


      bn2.close()
    }

    "The BayesianNetwork" should "allow registering of custom models" in {
      case class ConstantModel(value: Double) extends Model{
        def model(deps: org.apache.spark.sql.DataFrame, node: Node, count: Long): org.apache.spark.sql.DataFrame= {
          deps
        }

        override def asJson(): String = {
          s"""{"type": "ConstantModel", "value": $value}"""
        }
      }
      BayesianNetwork.registerModelType(classOf[ConstantModel])

      BayesianNetwork.modelTypes should contain (classOf[ConstantModel])
    }

}