package org.apache.spark.mllib.fpm


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
object FPRunner  {
  
  //https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/mllib/FPGrowthExample.scala
  def main(args: Array[String]) 
  {
    var start = System.currentTimeMillis  
    val inputPath = args(0)
    val outputPath = args(1)
    val resPath = args(2)
    val partsNum = args(3).toInt
    val minSup = args(4).toDouble
    val conf = new SparkConf().set("spark.executor.memory", "48G").setAppName("FPGRunner")
    //executor.memory是每个worker的
    //spark.cores.max是所有worker加起来最大的core数量  .set("spark.cores.max","160")
    //.set("spark.shuffle.manager","sort")
    val sc = new SparkContext(conf)
    val data = sc.textFile(inputPath,partsNum)
    val dataFormatted = data.map { line => line.split(',') }.cache()
    
    val numParts = dataFormatted.partitions.length
    println("分区数量是:"+numParts)
    val fpg = new FPGrowth()
    fpg.setNumPartitions(numParts)
    fpg.setMinSupport(minSup)
   // fpg.setMinCount(8888)
    
    println("模型计算开始:")
    val model = fpg.run(dataFormatted)
    println("模型计算结束!")
    println("模型计算时间 " + (System.currentTimeMillis - start) + " ms")
    
    //频繁项集挖掘
    println("保存频繁项集")
    val res = model.freqItemsets.map(row=>row._2+":"+row._1.mkString(",")) 
    val resNum = res.count()
    println("结果数量:"+resNum) 
    res.saveAsTextFile(outputPath)
    //频繁项集结果
    
    
    //规则提取
    println("规则提取开始:")
    start = System.currentTimeMillis 
    val rules = model.freqItemsets.flatMap{row=>
      val set = row._1   
      val kv = set.map{k => 
        val tk = k
        (tk,set.filter { v => tk!=v }.toSet)}
      kv     
    }.reduceByKey((a,b)=>
      a++b
    ,8)//保存再8个分区
    println("规则提取时间 " + (System.currentTimeMillis - start) + " ms")
    
    println("保存")
    rules.map(row=>row._1+":"+row._2.mkString(",")).saveAsTextFile(resPath)  //规则结果
    println("保存时间 " + (System.currentTimeMillis - start) + " ms")
    
    
    sc.stop()
    

  }
  
  
  
  
  
  
  
  
  

}