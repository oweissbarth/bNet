package de.oweissbarth.model
import de.oweissbarth.graph.Node
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.apache.spark.sql.functions._

/** a simple linear model that holds multiple hyperplane for different category combinations
  *
  * @param parameters the parameters of the linear model
  */
case class SimpleLinearModel(parameters: Map[Set[String], SimpleLinearModelParameterSet]) extends IntervalModel{
  override def model(dependencies: DataFrame, node: Node,  count: Long): DataFrame = {

    val categoricalParents = node.parents.filter(_.isCategorical).map(_.label)
    val intervalParents =  node.parents.filterNot(_.isCategorical).map(_.label)

    val modelApply = udf((r: Row)=>{
      val categories = r.getValuesMap[String](categoricalParents).values.toSet
      val parameters = this.parameters(categories)
      val result = r.getValuesMap[Double](intervalParents).map{case (label, value)=> parameters.slope(label)*value}
      result.sum +parameters.intercept
    })

    dependencies.withColumn(node.label, modelApply(struct(dependencies.columns.map(col(_)):_*)))
  }

}

// TODO we store the order of slope variables multiple times
case class SimpleLinearModelParameterSet(slope: Map[String, Double], intercept: Double)

object SimpleLinearModel extends Persist[SimpleLinearModel] {
  /** creates a new Model from json
    * @todo error handeling
    *
    * @param json the json input
    */
  override def fromJson(json: String): SimpleLinearModel = {
    /*val logger = LogManager.getLogger("SimpleLinearModel from Json")*/
    implicit val formats = Serialization.formats(ShortTypeHints(List(classOf[SimpleLinearModel])))
    parse(json).extract[SimpleLinearModel]
  }
}