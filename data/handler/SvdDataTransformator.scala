package data.handler


import scala.io.Source
import scala.collection.mutable.TreeSet
//import scala.collection.mutable.Map
//import scala.collection.mutable.HashMap
import java.util.TreeMap
import java.util.Map.Entry
import java.io.PrintWriter

object SvdDataTransformator {
  def main(args: Array[String]) 
  {
    val srcDataFile= "/home/linger/data/movie_data_set.dat"  //args(0)
    val dstDataFile="/home/linger/data/movie_data_set.svd_input"  //args(1)
    var userIds= new TreeSet[String]()
    var itemIds = new TreeSet[String]()
    
    var start = System.currentTimeMillis
    
    val lines = Source.fromFile(srcDataFile)
    var lineIter = lines.getLines()
    for(iter<-lineIter)
    {
        val fields = iter.toString().split(',')
        userIds+=fields(0) 
        itemIds+=fields(1)
    }
    var i= -1
    val usersMap = userIds.map { x => 
      i+=1
      (x,i) }.toMap
    
    
    
    i= -1
    val itemsMap = itemIds.map { x => 
      i+=1
      (x,i) }.toMap
    
    println("ID映射所花时间 " + (System.currentTimeMillis - start) + " ms") 
    println("user num:"+userIds.size)
    println("item num:"+itemIds.size)
    //usersMap.foreach{println}
    println("------------------------------------")
   // itemsMap.foreach{println}
    
   // userIds.foreach { println }
    //itemIds.foreach { println }
    
    start = System.currentTimeMillis
    
    var matrix = new Array[ TreeMap[Int,Double] ](userIds.size)
    
    lineIter = lines.reset().getLines()
    for(iter<-lineIter)
    {
        val fields = iter.toString().split(',')
        val userId = usersMap(fields(0))
        val itemId = itemsMap(fields(1))
       // println(userId,itemId,fields(2))
        
        var row = matrix.apply(userId)
        if(row==null)
        {
          row =  new TreeMap[Int,Double]()    
          matrix.update(userId, row)      
        }
        row.put(itemId, fields(2).toDouble)
    }  
      
    println("矩阵转换所花时间 " + (System.currentTimeMillis - start) + " ms")
    
    start = System.currentTimeMillis
     val writer = new PrintWriter(dstDataFile)
    matrix.foreach{row=>
      val iter =row.entrySet().iterator()
      while(iter.hasNext())
      {
         val entry = iter.next()
         val key = entry.getKey()   
         val value = entry.getValue()
        // print(key+":"+value+",")
         writer.write(key+":"+value+",")
      }
      //println()
      writer.write("\n")
    }
     
     writer.close()
    
    println("矩阵保存所花时间 " + (System.currentTimeMillis - start) + " ms")
    
  }
}