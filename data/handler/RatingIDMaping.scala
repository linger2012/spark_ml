package data.handler

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.Path._

import scala.collection.mutable.HashMap


import data.handler.utils._



/*
 * spark-submit --class "data.handler.RatingIDMaping" --driver-memory 10G RatingIDMaping.jar svd/movie_data_set.dat svd/svdInput 100
 */

object RatingIDMaping {
  def main(args: Array[String]) 
  {
    val srcDataFile= args(0) //"/home/linger/data/movie_data_set.dat"
    val output= args(1) 
    val trainRate=args(2).toInt //100
    val randomUtil = new scala.util.Random
    
    
    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)
    
    val reader = new HdfsReader(fileSystem)
    reader.open(srcDataFile)
    
    val trainWriter = new HdfsWriter(fileSystem)
    trainWriter.create(output+"/training.dat")
    
    val testWriter = new HdfsWriter(fileSystem)
    testWriter.create(output+"/testing.dat")   
    
    
    var line:String = null
  
    val userIndex =  new HashMap[String,Int]()
    val itemIndex =  new HashMap[String,Int]()
    var userIdInc= -1
    var itemIdInc= -1
    
    var cur_user=0
    var cur_item=0
    
    var start = System.currentTimeMillis
    var count=0
    
    while(reader.hasNext)
    {
        line=reader.next
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
        if(count%100000==0) println("count:"+count)
      
    }
    
    reader.close
    trainWriter.close
    testWriter.close

     println("count:"+count)
     println("转换所花时间 " + (System.currentTimeMillis - start) + " ms")
     
     start = System.currentTimeMillis
     
     val userMappingWriter = new HdfsWriter(fileSystem)
     userMappingWriter.create(output+"/UserMapping.dat")
     for((k,v)<-userIndex)
     {
       userMappingWriter.write(k+","+v+"\n")
     }
    
     val itemMappingWriter = new HdfsWriter(fileSystem)
     itemMappingWriter.create(output+"/ItemMapping.dat")
     for((k,v)<-itemIndex)
     {
       itemMappingWriter.write(k+","+v+"\n")
     }    
     
     userMappingWriter.close
     itemMappingWriter.close
     
     println("保存ID映射所花时间 " + (System.currentTimeMillis - start) + " ms")
  }
}