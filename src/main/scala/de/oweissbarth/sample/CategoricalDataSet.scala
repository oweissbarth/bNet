package de.oweissbarth.sample


class CategoricalDataSet(val dataFields: List[CategoricalField], override val label: String, val categories: CategorySet) extends  DataSet(label:String){

  override def toString() = {
    "CategoricalDataSet: "+label
  }
}
