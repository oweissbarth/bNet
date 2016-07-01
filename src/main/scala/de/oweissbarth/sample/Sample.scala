package de.oweissbarth.sample

import org.apache.spark.sql.DataFrame

// TODO switch to DataSet once ready +  supported by spark-csv
/** holds a DataFrame to model against
  *
  * @param records the data sample
  */
class Sample(val records: DataFrame) {
  override def toString() = {
    "Sample: "
  }
  
}