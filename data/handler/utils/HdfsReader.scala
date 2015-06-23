package data.handler.utils

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path._
import java.io.BufferedReader
import java.io.InputStreamReader


class HdfsReader(hdfs:FileSystem) extends Reader {
  
  var reader:BufferedReader=null
  var line:String=null
  def open(filePath:String):Boolean=
  {
      val fin = hdfs.open(new Path(filePath))
      reader = new BufferedReader(new InputStreamReader(fin))
      reader!=null
  }
  
  def hasNext:Boolean=
  {
     line = reader.readLine()
     line!=null && !line.isEmpty()
  }
  
  def next:String =  line
  
  def close:Boolean=
  {
    reader.close()
    true
  }
}