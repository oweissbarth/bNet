package de.oweissbarth.util

import de.oweissbarth.sample._
import org.apache.spark.sql.{Encoder, Encoders}

object BayesianEncoders {
  implicit def categoricalFieldEncoder: Encoder[CategoricalField] = Encoders.kryo[CategoricalField]

  implicit def intervalFieldEncoder: Encoder[IntervalField] = Encoders.kryo[IntervalField]

  implicit def dataFieldEncoder: Encoder[DataField] = Encoders.kryo[DataField]

  implicit def categoryEncoder: Encoder[Category]  = Encoders.kryo[Category]

  implicit def recordEncoder: Encoder[Record]  = Encoders.kryo[Record]



  /*  implicit def toFloat(i: IntervalField): Float = i.value
    implicit def toIntervalField(v: Float): IntervalField = new IntervalField(v)

    implicit def toString(c: CategoricalField): String = c.category.name
    implicit def toCategoricalField(s: String): CategoricalField = new CategoricalField(new Category(s))*/





}
