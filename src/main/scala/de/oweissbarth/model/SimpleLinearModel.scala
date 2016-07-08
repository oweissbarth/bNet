package de.oweissbarth.model
import org.apache.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.json4s._
import org.json4s.jackson.JsonMethods._

/** a simple linear model that holds multiple hyperplane for different category combinations
  *
  * @param parameters the parameters of the linear model
  */
class SimpleLinearModel(val parameters: Map[String, (Vector, Double)]) extends Model{
  override def model(dependencies: DataFrame): Unit = {}

  override  def asJson() = {
    s"""{"SimpleLinearModel": {"parameters": {${parameters.map(e=>s""""${e._1}": {"gradient": ${e._2._1}, "intercept": ${e._2._2}""").reduce(_+", "+_)}}}}}"""
  }
}

object SimpleLinearModel extends Persist[SimpleLinearModel] {
  /** creates a new Model from json
    *
    */
  override def fromJson(json: String): SimpleLinearModel = {
    val logger = LogManager.getLogger("SimpleLinearModel from Json")

    val ast = parse(json)
    val values = for{
      JObject(params) <- ast
      JField("gradient", JArray(gradient))<-params
      JDouble(co)   <- gradient
      JField("intercept", JDouble(intercept)) <-params
    }yield (gradient->intercept)


    new SimpleLinearModel(values)
  }
}