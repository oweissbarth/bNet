package de.oweissbarth.model

import de.oweissbarth.graph.Node
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.SparkContext
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions.{randn, udf}



/** hold the parameters of the simple 2 dimensional gaussian model
  *
  * @constructor creates a new gaussian model
  * @param expectation is turning point of the gaussian
  * @param variance is the variance of the gaussian
  */
case class GaussianBaseModel(expectation: Double, variance: Double) extends IntervalModel{
  override def model(dependencies: DataFrame, node: Node, count: Long): DataFrame = {
    val sc = SparkContext.getOrCreate()
    val sqlc = new SQLContext(sc)

    val modelApply = udf((d: Double) => d*Math.sqrt(variance)+expectation)

    dependencies.withColumn(node.label, modelApply(randn()))

  }

}

object GaussianBaseModel extends Persist[GaussianBaseModel]{
  /** creates a new GaussianBaseModel from json
    *
    */
  def fromJson(json: String): GaussianBaseModel = {
    /*val logger = LogManager.getLogger("GaussianBaseModel from Json")*/

    implicit val formats = DefaultFormats


    parse(json).extract[GaussianBaseModel]
  }

}
