package de.oweissbarth.sample

abstract class DataSet(val label: String) {
    override def toString() = {
      "DataSet: "+label
    }
}