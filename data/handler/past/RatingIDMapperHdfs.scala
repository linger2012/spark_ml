package data.handler


import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path._

import scala.util.control.Breaks._
import scala.collection.mutable.HashMap

object RatingIDMapperHdfs {
  def main(args: Array[String]) 
  {
    val srcDataFile= args(0) //"/home/linger/data/movie_data_set.dat"
    val trainFile= args(1) // "/home/linger/data/movie_data_set.rating_training"
    val testFile=args(2) //"/home/linger/data/movie_data_set.rating_testing"
    val trainRate=args(3).toInt //100
    val randomUtil = new scala.util.Random
    
    
    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)
    
    
    
    val fin = fileSystem.open(new Path(srcDataFile))
    val reader = new BufferedReader(new InputStreamReader(fin))
    
    val trainWriter = new PrintWriter(fileSystem.create(new Path(trainFile)))
    val testWriter = new PrintWriter(fileSystem.create(new Path(testFile)))

    
    var line:String = null
    
    val userIndex =  new HashMap[String,Int]()
    val itemIndex =  new HashMap[String,Int]()
    var userIdInc= -1
    var itemIdInc= -1
    
    var cur_user=0
    var cur_item=0
    
    val start = System.currentTimeMillis
    var count=0
    breakable
    {
      while(true)
      {
        line=reader.readLine()
        if(line==null) break
        val fields = line.split(',')    
        cur_user = if(userIndex.isDefinedAt(fields(0)))
        {
         userIndex(fields(0))
        }
        else 
        {
         userIdInc+=1
         userIndex(fields(0))=userIdInc
         userIdInc
        }

        cur_item = if(itemIndex.isDefinedAt(fields(1)))
        {
          itemIndex(fields(1))
        }
        else 
        {
          itemIdInc+=1
          itemIndex(fields(1))=itemIdInc
          itemIdInc
        }
        
        //0-99
        if(randomUtil.nextInt(100)<trainRate)
        {
           trainWriter.write(cur_user+","+cur_item+","+fields(2)+"\n")
        }
        else 
        {
          testWriter.write(cur_user+","+cur_item+","+fields(2)+"\n")
        }
        
        count+=1
        if(count%10000==0) println("count:"+count)
      }
    }
    
    reader.close()
    trainWriter.close()
    testWriter.close()

     println("count:"+count)
     println("转换所花时间 " + (System.currentTimeMillis - start) + " ms")
    
  }
}