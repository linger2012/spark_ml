package org.apache.spark.mllib.svd

import org.apache.spark.SparkContext._ 
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.cf.RatingInput

class SVDInput   {   
    
    var rows: IndexedRowMatrix=null
    var triples: RDD[(Long, Long, Double)]=null
    
    def this(sc: SparkContext,ratingFile:String)=
    {
      this()
      val ri = new RatingInput(sc,ratingFile)
      rows = ri.rows
      triples = ri.triples
    }

    def this(sc: SparkContext,ratingFile:String,numParts:Int)=
    {
      this()
      val ri = new RatingInput(sc,ratingFile,numParts)
      rows = ri.rows
      triples = ri.triples
    }
        
    def get = rows
    
    def rowMean = 
    {         
      val rowGroup = triples.map(triple=>(triple._1,triple._3))     
      val rowSum = rowGroup.reduceByKey((a,b)=>a+b)
      val rowCount = triples.map(triple=>(triple._1,1)).reduceByKey((a,b)=>a+b)
      val rowMean = rowSum.join(rowCount).map(f=>(f._1,f._2._1/f._2._2))    
      rowMean.collect().toMap
    }
    
    def colMean = 
    {
      val colGroup = triples.map(triple=>(triple._2,triple._3))
      val colSum = colGroup.reduceByKey((a,b)=>a+b)
      val colCount = triples.map(triple=>(triple._2,1)).reduceByKey((a,b)=>a+b)
      val colMean = colSum.join(colCount).map(f=>(f._1,f._2._1/f._2._2)) 
      colMean.collect().toMap
    }
    
    def getNorm=
    {
      
        val sc: SparkContext = rows.rows.context
        
        val rowmean=rowMean
        val colmean=colMean
        
        println("rows mean:")
        println(rowmean)
        
        println("cols mean:")
        println(colmean)
               
        val bcColmean = sc.broadcast(colmean)
        val bcRowmean = sc.broadcast(rowmean)
        
        val indexRowMatix = get      
        
        val indexedRows = indexRowMatix.rows.map { row =>  
          val colmeans = bcColmean.value
          val rowmeans = bcRowmean.value
          val rowIndex =row.index
          val rowVector =row.vector
          val denseVector = rowVector.toBreeze.toDenseVector
          val rm = rowmeans(rowIndex)
          val dv = denseVector.mapPairs((index,value)=>            
               if(value == 0.0)
               {
                 if(colmeans.contains(index))//我把数据集分成了训练集和测试集,可能这个key(item)在训练集不存在,而在测试集存在
                   colmeans(index) - rm
                 else 0.0
               }
               else 
               {              
                 value - rm
               }
            )
          (rowIndex,Vectors.fromBreeze(dv))         
        }.map(row=>IndexedRow(row._1,row._2))

        new IndexedRowMatrix(indexedRows)
    }
    
    
    
    
}