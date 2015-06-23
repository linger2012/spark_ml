package org.apache.spark.mllib.cf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.rdd.RDD

class RatingInput
{
    var rows: IndexedRowMatrix=null
    var lines: RDD[String]=null
    var triples: RDD[(Long, Long, Double)]=null
    def this(sc: SparkContext,ratingFile:String,isT:Boolean=false)=
    {
      this()
      lines = sc.textFile(ratingFile)
      toMatrix(isT)
    }
    
    def this(sc: SparkContext,ratingFile:String,numParts:Int,isT:Boolean=false)=
    {
      this()
      lines = sc.textFile(ratingFile,numParts)
      toMatrix(isT)
    }
    
    def toMatrix(isT:Boolean=false)=
    {
      
      triples = if(isT) lines.map(_.split(',')).map(_.map(_.toDouble)).map(m => (m(1).toLong,m(0).toLong,m(2)))
                else lines.map(_.split(',')).map(_.map(_.toDouble)).map(m => (m(0).toLong,m(1).toLong,m(2)))
      val sparseMatrix = triples.map(k => new MatrixEntry(k._1,k._2,k._3))
      val mat: CoordinateMatrix = new CoordinateMatrix(sparseMatrix)
      rows = mat.toIndexedRowMatrix 
    }

    def get = rows
}