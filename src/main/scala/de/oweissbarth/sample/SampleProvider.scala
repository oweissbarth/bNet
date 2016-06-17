package de.oweissbarth.sample

import org.apache.spark.sql.SQLContext

abstract class SampleProvider{
	def getSample(): Sample;
}
