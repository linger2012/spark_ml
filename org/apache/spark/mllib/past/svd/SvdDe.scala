package org.apache.spark.mllib.letv

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, axpy => brzAxpy,
  svd => brzSvd,Transpose}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{BLAS}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD



object SvdDe {
  
  
  
  def runSvd(rows: RDD[Vector],svdOut:String,svdNum:Int)=
  {

    
    val mat = new RowMatrix(rows)
    val svd = mat.computeSVD(svdNum,true)//mat.numCols().toInt
    
  }
  def main(args: Array[String]) 
  {
    
   val svd_input = args(0) // "/home/linger/dev/spark-1.2.0-bin-hadoop1/data/mllib/sparse_matrix.data" //
   val svd_output = args(1)// ""//
   val svd_num = args(2).toInt// 2 //
   
   val conf = new SparkConf().setAppName("SVD Application")  //
   val sc = new SparkContext(conf)

   val data = sc.textFile(svd_input)
   val matrix = data.map(_.split(',').map(_.split(':') match{
      case Array(index,value)=>(index.toInt,value.toDouble)
    }))
    
   
    val rowSize = matrix.map{array=>
      array.map(_._1).max+1
   }
   val itemSize = rowSize.max()
   println("itemSize:"+itemSize)
   val matrixWithSize = matrix.map{array=>
     Vectors.sparse(itemSize,array)
      }
   
    runSvd(matrixWithSize,svd_output,svd_num)   
    println("SVD finished")
    sc.stop()
    
  }
}