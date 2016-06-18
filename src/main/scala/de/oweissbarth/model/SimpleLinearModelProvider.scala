package de.oweissbarth.model

import de.oweissbarth.graph.Node
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap

class SimpleLinearModelProvider extends ModelProvider {
  def getModel(subDataSet: DataFrame, parents : Array[Node]): SimpleLinearModel = {

    val sc = SparkContext.getOrCreate()
    val sqlc = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val logger = Logger.getLogger(s"SimpleLinearModelProvider for ${subDataSet.columns(0)}")


    var parameters : Map[String, (Vector, Double)] = new HashMap()


    val categoricalParents = parents.filter(n => n.modelProvider.getOrElse(false).isInstanceOf[CategoricalModelProvider]).map(n=>n.label).map(label=> subDataSet.col(label))

    logger.info(s"Found ${categoricalParents.length} categorical parents.")


    if(categoricalParents.length  > 0){
      val categoryCombinations = subDataSet.select(categoricalParents:_*).distinct().map(r=>r.toSeq.map(e=>e.toString))
      logger.info(s"Fitting models for ${categoryCombinations.count()} category combinations.")
      categoryCombinations.foreach(c=>{
        val conditional = categoricalParents.map(a=>a.toString()).zip(c).map(c=>c._1+"="+c._2).reduceLeft((c1, c2)=>c1+"AND"+c2)
        val data = subDataSet.where(conditional).map(r=> LabeledPoint(r.getDouble(0), Vectors.dense(r.toSeq.toArray.map(
          _ match{
            case d: Double =>d
            case default => Double.NaN
          } ))))
        val lr = new LinearRegression()
        val result = lr.fit(sqlc.createDataFrame(data))
        val newParam = (c.toString() ->(result.coefficients, result.intercept))
        parameters = parameters + newParam
      })

    }else{
      logger.info("Fitting single model for interval parents.")
      val data = subDataSet.map(r=> LabeledPoint(r.getDouble(0), Vectors.dense(r.toSeq.drop(1).toArray.map(
        _ match{
          case d: Double =>d
          case default => Double.NaN
        } ))))
      val lr = new LinearRegression()
      val result = lr.fit(sqlc.createDataFrame(data))
      val newParam = ("" ->(result.coefficients, result.intercept))
      parameters = parameters + newParam
    }

    return new SimpleLinearModel(parameters)
  }
}