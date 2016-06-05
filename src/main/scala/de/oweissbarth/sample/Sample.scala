package de.oweissbarth.sample

class Sample(val dataSets: List[DataSet]) {
  override def toString() = {
    "Sample: "+dataSets.foldLeft("")((a, b) => a+" |"+b)
  }
  
}