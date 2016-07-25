package de.oweissbarth.model
import de.oweissbarth.graph.Node
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{rand, udf}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s._

/** a categorical model with no dependencies
  *
  * @constructor  creates a simple catgorical model
  * @param distribution the propability of each category
  */
case class SimpleCategoricalModel(distribution: Map[String, Double]) extends CategoricalModel {
  override def model(dependencies: DataFrame, node: Node, count : Long): DataFrame = {

    val sc = SparkContext.getOrCreate()
    val sqlc = new SQLContext(sc)


    def aggragateClasses(l: List[(String, Double)]): List[(Double, String)] = {
      var agg  = 0.0
      for(e <- l )
        yield ({agg += e._2; agg}, e._1)

    }

    val aggregated = aggragateClasses(distribution.toList)


    val modelApply = udf((d: Double)=> aggregated.filter(_._1>=d).head._2)



    dependencies.withColumn(node.label, modelApply(rand()))

  }


  /** returns a human readable representation of the model
    *
    * @return a human readable representation of the model
    */
  override def toString(): String = {
    "SimpleCategoricalModel: <" + distribution.foreach(p => p.toString) + ">"
  }

}

object SimpleCategoricalModel extends Persist[SimpleCategoricalModel] {
  /** creates a new Model from json
    *
    */
  override def fromJson(json: String): SimpleCategoricalModel = {
   /* val logger = LogManager.getLogger("SimpleCategoricalModel from Json")*/
   implicit val formats = DefaultFormats

    parse(json).extract[SimpleCategoricalModel]
  }
}
