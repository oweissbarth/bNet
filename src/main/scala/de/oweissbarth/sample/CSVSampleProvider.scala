package de.oweissbarth.sample

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.SparkContext

import scala.util.Try

/** supplies a Sample as read from a csv file
  *
  * @note the csv file has to have a header
  *
  * @param filepath where the csv file is stored
  * @param delimiter how elements are split within a row
  */
class CSVSampleProvider(filepath :String, delimiter: String ) extends SampleProvider{



	override def getSample(): Sample = {
    val sc = SparkContext.getOrCreate()
    val sqlc = SQLContext.getOrCreate(sc)

    val file = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", delimiter).option("inferSchema", "true").load(filepath)
    
    new Sample(file)

	}
}
