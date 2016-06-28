package de.oweissbarth

import de.oweissbarth.sample._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import org.scalatest._

class SampleTest extends FlatSpec with BeforeAndAfterAll with MustMatchers{


  override def beforeAll() = {
    val sparkConf = new SparkConf()

    val sc = new SparkContext("local", "Baysian", sparkConf)

    val sqlc = new SQLContext(sc)

  }

  override def afterAll() = {
    val sc = SparkContext.getOrCreate()
    sc.stop()
  }

  "An IntervalField" should "construct without Exceptions and be non null" in{
    val interval = new IntervalField(2.4f)
    assert(interval != null)
  }

  "An CategoricalField" should "construct without Exceptions and be non null" in{
    val categorySet = new CategorySet()
    val categorical = new CategoricalField(categorySet.get("Male"))
    assert(categorical != null)
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

    assert(recordFields(0).isInstanceOf[Double])
    assert(recordFields(1).isInstanceOf[String])
    assert(recordFields(2).isInstanceOf[Double])
  }




}