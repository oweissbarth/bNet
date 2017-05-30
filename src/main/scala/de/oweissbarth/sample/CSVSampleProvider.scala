/*
 * Copyright 2017 Oliver Weissbarth
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
