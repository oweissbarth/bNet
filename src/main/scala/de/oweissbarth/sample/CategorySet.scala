package de.oweissbarth.sample

import scala.collection.SetLike

class CategorySet(){

  var category = Set[Category]()

  def get(name: String): Category ={
    category.find(a=> a.name == name).getOrElse( {
      val n = new Category(name)
      category = category + n
      n
    })
  }

  def size()={
    category.size
  }
}
