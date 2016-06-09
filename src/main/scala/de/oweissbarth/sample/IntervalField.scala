package de.oweissbarth.sample

class IntervalField(val value: Float) extends DataField{
  override def toString() ={
    "\t"+value+"\n"
  }
}