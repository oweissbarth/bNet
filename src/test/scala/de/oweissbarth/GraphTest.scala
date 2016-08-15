package de.oweissbarth

import de.oweissbarth.graph.DirectedAcyclicGraph.GraphIsCyclicException
import de.oweissbarth.graph._
import de.oweissbarth.model._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

import scala.collection.immutable.HashMap


class GraphTest extends FlatSpec with BeforeAndAfterAll with Matchers{

    override def beforeAll() = {
      val sparkConf = new SparkConf()

      val sc = new SparkContext("local", "Baysian", sparkConf)

      val sqlc = new SQLContext(sc)

    }

    override def afterAll() = {
      val sc = SparkContext.getOrCreate()
      sc.stop()
    }
    
    "A Node" should " be constructable without error" in{
      new Node("Test") should not be null
    }

    it should "return the label by which it was constructed" in {
      new Node("Test").label should be ("Test")
    }

    it should "serialize to String" in {
      new Node("Test").toString() should be ("Node: Test [parents: ]")
    }

  it should "return the correct model type" in {
    val n = new Node("test")

    n.isCategorical() should be (false)

    n.modelProvider = Option(new SimpleCategoricalModelProvider())
    n.isCategorical() should be (true)

    n.modelProvider = Option(new SimpleLinearModelProvider())
    n.isCategorical() should be (false)

    n.modelProvider = None
    n.model = Option(SimpleCategoricalModel(Map("A"->0.5)))
    n.isCategorical() should be (true)

    n.model = Option(GaussianBaseModel(0.5, 3.8))
    n.isCategorical() should be (false)
  }

    
    " A Directed acyclic graph" should "construct from a list of labels and edges" in {
      val labels = List("1", "2", "3", "4")
      val edges = List((0,1), (0,3), (2,3), (3,1))

      val graph = DirectedAcyclicGraph.fromLabelsAndEdges(labels, edges)

      graph.nodes should have size 4

      graph.nodes("1").parents should have length 0

      graph.nodes("2").parents should have length 2
      graph.nodes("2").parents(0).label should be ("1")
      graph.nodes("2").parents(1).label should be ("4")

      graph.nodes("3").parents should have length 0

      graph.nodes("4").parents should have length 2
      graph.nodes("4").parents(0).label should be ("1")
      graph.nodes("4").parents(1).label should be ("3")

    }

    it should "return a node by a given label" in {
      val node = new Node("Test")
      val graph = new DirectedAcyclicGraph(HashMap("Test"->node))

      graph.getNodeByLabel("Test") should be (node)
    }

    it should "serialize to String" in {
      val graph = DirectedAcyclicGraph.fromLabelsAndEdges(List(), List())
      graph.toString should be ("DirectedAcyclicGraph: Map()")
    }
    
    "A GraphMLGraphProvider" should "read a file and construct a correct dag" in {
      val pro = new GraphMLGraphProvider("src/test/resources/example_graph.gml")


      val graph = pro.getGraph()


      graph.nodes should have size 4

      graph.nodes("1").parents should have length 0

      graph.nodes("2").parents should have length 2
      graph.nodes("2").parents(0).label should be ("1")
      graph.nodes("2").parents(1).label should be ("4")

      graph.nodes("3").parents should have length 0

      graph.nodes("4").parents should have length 2
      graph.nodes("4").parents(0).label should be ("1")
      graph.nodes("4").parents(1).label should be ("3")
    }


    "A GraphIsCyclicException" should "be instantiatable" in {
      val e = GraphIsCyclicException()
      e should not be (null)
      e shouldBe an [Exception]

  }

    "the isValid method " should " return true for a acyclic graph" in {
      val proValid = new GraphMLGraphProvider("src/test/resources/example_graph.gml")

      val graphValid = proValid.getGraph()
      graphValid should not be null

      val proInvalid = new GraphMLGraphProvider("src/test/resources/example_graph_cyclic.gml")

      an[DirectedAcyclicGraph.GraphIsCyclicException] should be thrownBy ( {val graphInvalid = proInvalid.getGraph()})
    }

}