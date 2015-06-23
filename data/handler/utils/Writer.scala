package data.handler.utils

abstract class Writer {
  def create(filePath:String):Boolean
  def write(str:String):Boolean
  def close:Boolean
}