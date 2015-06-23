package org.apache.spark.mllib.apriori


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object Apriori {
  


  def main(args: Array[String]) 
  {
    val inputPath = args(0)
    val outputPath = args(1)
    val min_support = args(2).toInt
    
    val conf = new SparkConf().set("spark.executor.memory", "48G").setAppName("Apriori")
    val sc = new SparkContext(conf)
    
    val input = sc.textFile(inputPath)
    
    val transactions = input.map(_.split(",").map(_.toInt)).cache
    val oneFIS = transactions.flatMap(line=>line).map((_, 1)).reduceByKey(_ + _).filter(_._2 > min_support)
  
    val keySet = oneFIS.map(_._1).collect()  
    var candidate = scala.collection.mutable.Set[Set[Int]]()  
    for (i <- keySet) 
    {  
      for (j <- keySet) 
      {  
        candidate += Set(i, j)  
      }  
    }
    
    def support(trans: Set[Int], set: Set[Int]): Int = {
      var contain = 1
      set.find(item => 
        {
          if (!trans.contains(item)) 
          {
            contain = 0
            true
          }
          else
            false
        })
      contain
    }
      
    def validateCandidate(candidate: scala.collection.mutable.Set[Set[Int]]) = {
      transactions.flatMap(line => {
          var tmp = scala.collection.mutable.Set[(Set[Int], Int)]()
          for (i <- candidate) {
            tmp += i -> support(line.toSet, i)
          }
          tmp
        }).reduceByKey(_ + _).filter(_._2 > min_support)
    }
    
    val twoFIS = validateCandidate(candidate)
    println("count:"+twoFIS.count())
    
    twoFIS.map(row=>
      row._1.mkString(",")).saveAsTextFile(outputPath)
    
    
  }
}