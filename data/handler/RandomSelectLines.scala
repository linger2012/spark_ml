package data.handler


import scala.io.Source
import scala.util.control.Breaks._

import java.io.PrintWriter

object RandomSelectLines {
  def main(args: Array[String]) 
  {
    val fileName="/home/linger/data/movie_data_set.dat"   //args(0)
    val outName="/home/linger/data/movie_data_set.svd_sample"  //args(1)
    
    //从文件的前m行随机挑选n行
    val m=55323361 //args(2).toInt 
    val n=1000000 //args(3).toInt 
    
    
    val lineIndex =collection.mutable.IndexedSeq.range(1, m+1)    //Vector.range(1,m+1)
    //lineIndex.foreach(println)
    
    
    val shouldCheck = false
    
    
    val randomUtil = new scala.util.Random
    println("产生随机数")
    for (i <- 0 to n-1)
    {
      var ranIndex = randomUtil.nextInt(m)//范围0-m-1
      var tmp = lineIndex.apply(i)
      //println(ranIndex,lineIndex(ranIndex))
      lineIndex.update(i, lineIndex(ranIndex))
      lineIndex.update(ranIndex,tmp)
      //println(lineIndex(i))
    }
    var selIndex = lineIndex.slice(0, n)
    selIndex=  selIndex.sortWith((first,second)=>first<second)
    
    var stackIndex = selIndex.map(_.toString).to[collection.mutable.Stack] //collection.mutable.Queue()
    //println(stackIndex)
    //println(stackIndex.top)
    //stackIndex.pop()
    //println(stackIndex)
    println("已确定挑选哪些行")
    //selIndex.foreach(println)
    

    
    val lines = Source.fromFile(fileName,"UTF-8")
    
    var lineIter = lines.getLines()
    
    if(shouldCheck)
    {
      val linesNum = lineIter.size
      println("文件行数:"+linesNum)
      if(m>linesNum)
      {
        println("m应该小于文件行数")
        return
      }
    }

    println("正在挑选")
    val start = System.currentTimeMillis
    lineIter = lines.reset().getLines()
    var curIndex=1
    val writer = new PrintWriter(outName)
    breakable
    {
      for(iter<-lineIter)
      {
        if(stackIndex.isEmpty)
        {
          println("已挑选完毕")
          break;
        }
                
        if(curIndex == stackIndex.top.toInt)
        {
          //println(iter)
          writer.println(iter)
          stackIndex.pop()
        }
        curIndex+=1
      }
    }
    
    writer.close()
    println("took " + (System.currentTimeMillis - start) + " ms")
    
    
  }
}