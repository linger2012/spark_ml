package data.handler

import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.PrintWriter

object RatingIDMapper {
  def main(args: Array[String]) 
  {
    
    
    //user也许不做mapping也行,但item一定要做.而且还要保证两者为long
    
    val srcDataFile= "/home/linger/dev/spark-1.2.0-bin-hadoop1/data/mllib/als/test.csv" // "/home/linger/data/ml_100k.ratings.dat" //"/home/linger/data/movie_data_set.dat"  //  args(0) //
    val trainFile=  "/home/linger/data/movie_data_set.rating_training" //args(1) //
    val testFile="/home/linger/data/movie_data_set.rating_testing" //args(2) //
    val trainRate=90
    val randomUtil = new scala.util.Random
    
    val trainWriter = new PrintWriter(trainFile)
    val testWriter = new PrintWriter(testFile)
    
    val userIndex =  new HashMap[String,Int]()
    val itemIndex =  new HashMap[String,Int]()
    var userIdInc= -1
    var itemIdInc= -1
    
    var cur_user=0
    var cur_item=0
    val lines = Source.fromFile(srcDataFile)
    var lineIter = lines.getLines()
    val start = System.currentTimeMillis
    var count=0
    for(iter<-lineIter)
    {
        val fields = iter.toString().split(",")    
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
          trainWriter.write(cur_user+","+cur_item+","+fields(2)+"\n")
        else testWriter.write(cur_user+","+cur_item+","+fields(2)+"\n")
        
        count+=1
        if(count%10000==0) println("count:"+count)
        
    }
    println("count:"+count)
    trainWriter.close()
    testWriter.close()
    println("user num:"+userIdInc)
    println("item num:"+itemIdInc)
    println("转换所花时间 " + (System.currentTimeMillis - start) + " ms")
  }
}