package data.handler.utils

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path._
import java.io.PrintWriter


class HdfsWriter(hdfs:FileSystem) extends Writer{
  
  var writer:PrintWriter=null
  def create(filePath:String):Boolean=
  {
    writer = new PrintWriter(hdfs.create(new Path(filePath)))
    writer!=null
  }
  
  def write(str:String):Boolean=
  {
    writer.write(str)
    true
  }
  
  def close:Boolean=
  {
    writer.close()
    true
  }
}