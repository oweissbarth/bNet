package de.oweissbarth

import de.oweissbarth.graph._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._


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
    

    "the isValid method " should " return true for a acyclic graph" in {
      val proValid = new GraphMLGraphProvider("src/test/resources/example_graph.gml")

      val graphValid = proValid.getGraph()
      graphValid should not be null

      val proInvalid = new GraphMLGraphProvider("src/test/resources/example_graph_cyclic.gml")

      an[DirectedAcyclicGraph.GraphIsCyclicException] should be thrownBy ( {val graphInvalid = proInvalid.getGraph()})
    }

}