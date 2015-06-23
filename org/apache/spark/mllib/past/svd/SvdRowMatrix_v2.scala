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



object SvdRowMatrix_v2 {
  
  def runSvd(rows: RDD[Vector],svdOut:String,svdNum:Int)=
  {

    
    val mat = new RowMatrix(rows)
    val svd = mat.computeSVD(svdNum,true)//mat.numCols().toInt
     
    val U = svd.U   
    val s = svd.s
    println("s:-------------------------------------------------------------------------")
   // println(s)
    println("s大小"+s.size)//2
    
    val Bs = U.rows.context.broadcast(s) //将s广播出去,可能广播array更好
    
    
    val UCols = U.numCols()        
    val matrix1 = U.rows.mapPartitions{iter=>
      val bs = Bs.value
      iter.map { row => 
        val v = BDV.zeros[Double](UCols.toInt)
        row.foreachActive((index,value)=>v(index)=value*bs(index))       
        Vectors.fromBreeze(v)     }
    }
      
    val leftMatrix = new RowMatrix(matrix1)
 
    
    //求V的转置,目前实现是线性的,但是维度不大
    val V = svd.V
    val vCols = V.numCols
    val vRows = V.numRows
    val vArray = V.toArray
    var vtArray = new Array[Double](vArray.size)
    val vtCols = vRows
    val vtRows = vCols
    for(i<-0 until vtArray.size)
    {
      val col=(i/vtRows).toInt
      val row=i%vtRows
      val o_col = row
      val o_row = col
      val o_index = o_col*vRows+o_row
      vtArray.update(i, vArray(o_index))    
    }
    
    //println(V)
    //V.toArray.foreach(println)
    //vtArray.foreach { println }
    val VT = new DenseMatrix(vtRows,vtCols,vtArray)
    //println(VT)
    
    val res = leftMatrix.multiply(VT)
    
    println("res:")
    //res.rows.collect().foreach(println)
    res.rows.saveAsTextFile(svdOut)
    
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