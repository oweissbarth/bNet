package de.oweissbarth.sample

import de.oweissbarth.core.BayesianSparkContext

import scala.io.Source
import scala.util.Try

class CSVSampleProvider(filepath :String, delimiter: String ) extends SampleProvider with BayesianSparkContext{
  val file = Source.fromFile(filepath)
  val head = file.getLines.next.split(delimiter)
  val columns = file.getLines.drop(1).toList.map(_.split(delimiter).toList).transpose
  val sample = new Sample((head, columns).zipped.map((label, col) => constructDataSetInferType(label, col)).toList)

	def getSample(): Sample = {
	  this.sample
	}

  private def constructDataSetInferType(label : String, col : List[String]): DataSet ={
    val setType = if(col.map((e)=>Try(e.toFloat).isFailure).reduce((a, b)=>a||b))  classOf[CategoricalDataSet] else classOf[IntervalDataSet] //TODO check performance of this. We are parsing to Float twice
    constructDataSet(label, col, setType)
  }

  private def constructDataSet(label: String, col: List[String], setType: Class[_]): DataSet ={
    if(setType == classOf[CategoricalDataSet]){
      val cSet = new CategorySet()
      new CategoricalDataSet(col.map(field=> new CategoricalField(cSet.get(field))), label, cSet)
    }else{
      new IntervalDataSet(col.map(field=> new IntervalField(field.toFloat)), label)
    }
  }
}
