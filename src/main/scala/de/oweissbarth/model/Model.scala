package de.oweissbarth.model

import org.apache.spark.sql.DataFrame

/** a model that holds all calaculated parameters
  *
  */
abstract class Model {
  /** applies the model to the given input parameters
    *
    * @param dependencies the variables of the mdel
    */
  def model(dependencies:  DataFrame)

  /** returns a json representation of the model
    *
    * @return a json representation of the model
    */
  def asJson(): String
}

abstract trait Persist[T /*<: Persist[T]*/]{ // TODO this is not working. No idea why
  /** creates a new Model from json
    *
    */
  def fromJson(json: String) : T

}