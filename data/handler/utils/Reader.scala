package data.handler.utils

abstract class Reader {
  def open(filePath:String):Boolean
  def hasNext:Boolean
  def next:String
  def close:Boolean
}