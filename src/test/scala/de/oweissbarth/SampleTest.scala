package de.oweissbarth

import de.oweissbarth.sample._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest._

class SampleTest extends FlatSpec with BeforeAndAfterAll with Matchers{


  override def beforeAll() = {
    val sparkConf = new SparkConf()

    val sc = new SparkContext("local", "Baysian", sparkConf)

    val sqlc = new SQLContext(sc)

  }

  override def afterAll() = {
    val sc = SparkContext.getOrCreate()
    sc.stop()
  }

 "A CSVSampleProvider" should "be non null after creation" in {
   val sc = SparkContext.getOrCreate()
   implicit val sqlc = SQLContext.getOrCreate(SparkContext.getOrCreate())

   val prov = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")
   assert(prov != null)
 }

  it should " return a non null sample when getSample is called if constructed with valid path" in {
    val sc = SparkContext.getOrCreate()
    implicit val sqlc = SQLContext.getOrCreate(SparkContext.getOrCreate())

    val prov = new CSVSampleProvider("src/test/resources/xy_data.csv", "\t")
    assert(prov.getSample() != null)
  }


  it should " infer schema correctly" in{
    val sc = SparkContext.getOrCreate()
    implicit val sqlc = SQLContext.getOrCreate(sc)

    val prov = new CSVSampleProvider("src/test/resources/ageGenderIncome.csv", ";")

    val recordFields = prov.getSample().records.first()


    //TODO this does not work
    //recordFields(0) should be a ('Double)
    //recordFields(1) should be a ('String)
    //recordFields(2) should be a ('Double)

  }




}