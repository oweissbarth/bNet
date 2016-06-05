package de.oweissbarth

import org.junit._
import Assert._

import de.oweissbarth.graph._

@Test
class GraphTest {
    
    @Test
    def testNodeCreation()={
      assertNotNull(new Node("Test"))
    }
    
    @Test
    def testFromLabelsAndEdges()={
      val graph = new DirectedAcyclicGraph()
      assertNotNull(graph)
      
      val labels = List("1", "2", "3", "4")
      val edges = List((0,1), (0,3), (2,3), (3,1))
      
      assertNotNull(graph.fromLabelsAndEdges(labels, edges))
      
      assertEquals(4, graph.nodes.size)
      
      assertEquals(0, graph.nodes("1").parents.length)
      
      assertEquals(2, graph.nodes("2").parents.length)
      assertEquals("1", graph.nodes("2").parents(0).label)
      assertEquals("4", graph.nodes("2").parents(1).label)
            
      assertEquals(0, graph.nodes("3").parents.length)
      
      assertEquals(2, graph.nodes("4").parents.length)
      assertEquals("1", graph.nodes("4").parents(0).label)
      assertEquals("3", graph.nodes("4").parents(1).label)

      
    }
    
    @Test
    def testGraphMLGraphProvider()={
      val pro = new GraphMLGraphProvider("src/test/resources/example_graph.gml")
      
      
      val graph = pro.getGraph()
      assertNotNull(graph)
      
      assertEquals(4, graph.nodes.size)
      
      assertEquals(0, graph.nodes("1").parents.length)
      
      assertEquals(2, graph.nodes("2").parents.length)
      assertEquals("1", graph.nodes("2").parents(0).label)
      assertEquals("4", graph.nodes("2").parents(1).label)
            
      assertEquals(0, graph.nodes("3").parents.length)
      
      assertEquals(2, graph.nodes("4").parents.length)
      assertEquals("1", graph.nodes("4").parents(0).label)
      assertEquals("3", graph.nodes("4").parents(1).label)

      
    }
    
    @Test
    def testIsAcyclic()={
      val proValid = new GraphMLGraphProvider("src/test/resources/example_graph.gml")
      
      val graphValid = proValid.getGraph()
      assertNotNull(graphValid)
      
      assertTrue(graphValid.isValid())
      
      val proInvalid = new GraphMLGraphProvider("src/test/resources/example_graph_cyclic.gml")
      
      val graphInvalid = proInvalid.getGraph()
      assertNotNull(graphInvalid)
      
      assertFalse(graphInvalid.isValid())
    }
}