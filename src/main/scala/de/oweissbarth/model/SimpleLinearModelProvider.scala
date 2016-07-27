package de.oweissbarth.model

import de.oweissbarth.graph.Node
import org.apache.log4j.Logger
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.linalg.Vectors
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


    import sqlc.implicits._

    var parameters : Map[String, SimpleLinearModelParameterSet] = new HashMap()

    val categoricalParents = parents.filter(_.isCategorical).map(_.label)
    val intervalParents = parents.filterNot(_.isCategorical).map(_.label)

    logger.info(s"Found ${categoricalParents.length} categorical parents.")


    if(categoricalParents.length  > 0){
      val categoricalColumns = categoricalParents.map(label=> subDataSet.col(label))
      val parentColumns = parents.map(node => subDataSet.col(node.label))
      val allColumns = (subDataSet.col(subDataSet.columns(0)))+:parentColumns
      val categoryCombinations = subDataSet.select(categoricalColumns:_*).distinct().map(r=>r.toSeq.map(e=>e.toString)).collect() // NOTE use of RDD here
      logger.info(s"Fitting models for ${categoryCombinations.length} category combinations.")
      categoryCombinations.foreach(c=>{
        val conditional = categoricalParents.map(a=>a.toString()).zip(c).map(c=>c._1+"=\'"+c._2+"\'").reduceLeft((c1, c2)=>c1+"AND"+c2)
        val selectedData = subDataSet.where(conditional).select(allColumns.diff(categoricalColumns):_*)
        val assembler = new VectorAssembler()
            .setInputCols(selectedData.columns.drop(1))
            .setOutputCol("features")
       /* val data = selectedData.map(r=> Vectors.dense(r.toSeq.toArray.map(
          _ match{
            case d: Double =>d
            case default =>/* logger.error(s"Value $default is not of type double. ");*/ Double.NaN
          } )))*/
        val lr = new LinearRegression()
        val result = lr.fit(assembler.transform(selectedData).withColumn("label", selectedData.col(selectedData.columns(0))))
        val newParam = (c.sorted.reduce(_+","+_) -> SimpleLinearModelParameterSet((intervalParents, result.coefficients.toArray).zipped.toMap, result.intercept))
        parameters += newParam
      })

    }else{
      logger.info("Fitting single model for interval parents.")
     /*val data = subDataSet.map(r=> Vectors.dense(r.toSeq.toArray.map(
        _ match{
          case d: Double =>d
          case default => Double.NaN
        } )))*/
     val assembler = new VectorAssembler()
       .setInputCols(subDataSet.columns.drop(1))
       .setOutputCol("features")
      val lr = new LinearRegression()
      val result = lr.fit(assembler.transform(subDataSet).withColumn("label", subDataSet.col(subDataSet.columns(0))))
      val newParam = ("" -> SimpleLinearModelParameterSet((intervalParents, result.coefficients.toArray).zipped.toMap, result.intercept))
      parameters += newParam
    }

    new SimpleLinearModel(parameters)
  }
}