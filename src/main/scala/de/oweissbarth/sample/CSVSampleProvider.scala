package de.oweissbarth.sample

import org.apache.spark.sql.{Row, SQLContext}
import de.oweissbarth.util.BayesianEncoders._


class CSVSampleProvider(filepath :String, delimiter: String ) extends SampleProvider{



	def getSample()(implicit sqlc: SQLContext ): Sample = {


    val file = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", delimiter).option("inferSchema", "true").load(filepath)


    new Sample(file)

	}

  /*private def constructDataSetInferType(label : String, col : List[String]): Record ={
    val setType = if(col.map((e)=>Try(e.toFloat).isFailure).reduce((a, b)=>a||b))  classOf[CategoricalDataSet] else classOf[IntervalDataSet] //TODO check performance of this. We are parsing to Float twice
    constructDataSet(label, col, setType)
  }

  private def constructDataSet(label: String, col: List[String], setType: Class[_]): Record ={
    if(setType == classOf[CategoricalDataSet]){
      val cSet = new CategorySet()
      new CategoricalDataSet(col.map(field=> new CategoricalField(cSet.get(field))), label, cSet)
    }else{
      new IntervalDataSet(col.map(field=> new IntervalField(field.toFloat)), label)
    }
  }*/
}
