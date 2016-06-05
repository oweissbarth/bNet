package de.oweissbarth.model

class LinearModelProvider extends ModelProvider {
  def getModel(): Model = {
    return new LinearModel()
  }
}