package de.oweissbarth.sample

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.SparkContext

import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}

import scala.util.Try

import scala.io.Source

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

    val src = Source.fromFile(filepath).getLines()
    val labels = src.next().split(delimiter).map(_.trim)
    var first = src.next().split(delimiter)

    val schema = labels zip first map({case (l :String, e:String )=> if(Try(e.toDouble).isSuccess){
                                                                          StructField(l, DoubleType, true)
                                                                    }else{
                                                                          StructField(l, StringType, true)
                                                                    }})
    val fullSchema = StructType(schema)

    val file = sqlc.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", delimiter).schema(fullSchema).load(filepath)
    
    new Sample(file)

	}
}
