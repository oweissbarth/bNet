package de.oweissbarth.model

import de.oweissbarth.graph.Node
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.immutable.HashMap

/** supplies a SimpleLinearModel to a bayesian Network
  *
  */
class SimpleLinearModelProvider extends ModelProvider {

  //TODO requires documentation
  def getModel(subDataSet: DataFrame, parents : Array[Node]): SimpleLinearModel = {

    val sc = SparkContext.getOrCreate()
    val sqlc = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val logger = Logger.getLogger(s"SimpleLinearModelProvider for ${subDataSet.columns(0)}")


    var parameters : Map[Set[String], SimpleLinearModelParameterSet] = new HashMap()

    val categoricalParents = parents.filter(_.isCategorical).map(_.label)
    val intervalParents = parents.filterNot(_.isCategorical).map(_.label)

    logger.info(s"Found ${categoricalParents.length} categorical parents.")


    if(categoricalParents.length  > 0){
      val categoricalColumns = categoricalParents.map(label=> subDataSet.col(label))
      val parentColumns = parents.map(node => subDataSet.col(node.label))
      val allColumns = (subDataSet.col(subDataSet.columns(0)))+:parentColumns
      val categoryCombinations = subDataSet.select(categoricalColumns:_*).distinct().map(r=>r.toSeq.map(e=>e.toString).toSet) // NOTE use of RDD here
      logger.info(s"Fitting models for ${categoryCombinations.count()} category combinations.")
      categoryCombinations.foreach(c=>{
        val conditional = categoricalParents.map(a=>a.toString()).zip(c).map(c=>c._1+"=\'"+c._2+"\'").reduceLeft((c1, c2)=>c1+"AND"+c2)
        val selectedData = subDataSet.where(conditional).select(allColumns.diff(categoricalColumns):_*)
        val data = selectedData.map(r=> LabeledPoint(r.getDouble(0), Vectors.dense(r.toSeq.toArray.drop(1).map(
          _ match{
            case d: Double =>d
            case default =>/* logger.error(s"Value $default is not of type double. ");*/ Double.NaN
          } ))))
        val lr = new LinearRegression()
        val result = lr.fit(sqlc.createDataFrame(data))
        val newParam = (c -> SimpleLinearModelParameterSet((intervalParents, result.coefficients.toArray).zipped.toMap, result.intercept))
        parameters += newParam
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
      val newParam = (Set[String]() -> SimpleLinearModelParameterSet((intervalParents, result.coefficients.toArray).zipped.toMap, result.intercept))
      parameters += newParam
    }

    new SimpleLinearModel(parameters)
  }
}