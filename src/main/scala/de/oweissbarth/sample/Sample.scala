package de.oweissbarth.sample

import org.apache.spark.sql.DataFrame

// TODO switch to DataSet once ready +  supported by spark-csv
class Sample(val records: DataFrame) {
  override def toString() = {
    "Sample: "
  }
  
}