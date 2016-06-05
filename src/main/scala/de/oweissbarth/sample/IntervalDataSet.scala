package de.oweissbarth.sample

class IntervalDataSet(val dataFields: List[IntervalField], override val label:String) extends  DataSet(label:String){
  override def toString() = {
    "IntervalDataSet: "+label
  }
}
