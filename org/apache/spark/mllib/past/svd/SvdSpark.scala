package org.apache.spark.mllib

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



object SvdSpark {
  def main(args: Array[String]) 
  {
    val filePath="/home/linger/dev/spark-1.2.0-bin-hadoop1/data/mllib/svd.data" 
    val conf = new SparkConf().setAppName("Svd Application")
    val sc = new SparkContext(conf)
    val rows = sc.textFile(filePath).map { 
               line => 
               val values = line.split(',').map(_.toDouble)
               Vectors.dense(values)}
    
    println("原矩阵-------------------------------------------------------------------------")
    rows.foreach { println}
      
    val mat = new RowMatrix(rows)
    val svd = mat.computeSVD(2,true)//mat.numCols().toInt
    
    
    val U = svd.U
    println("U:-------------------------------------------------------------------------")
    U.rows.foreach(println) //rdd打印出来不一定按常规顺序的,不要以为错了
    println("U列数"+U.numCols())//2
    println("U行数"+U.numRows())//6
    
    val s = svd.s
    println("s:-------------------------------------------------------------------------")
    println(s)
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
      
   // matrix1.foreach{println}
    matrix1.collect().foreach{println}
    
    
    val leftMatrix = new RowMatrix(matrix1)
    
    
    val V = svd.V
    println("V列数"+V.numCols)//2
    println("V行数"+V.numRows)//4
    println("V:-------------------------------------------------------------------------")
    //println(V)
    
    
   //不要尝试矩阵存储的转置,因为是没必要的----------不一定对,因为如果不转置,则数据访问没有那么方便
    //右边矩阵转置,则恰好符合spark的矩阵设计.做乘法时数据访问集中
    //右边矩阵不转置,则节省转置的时间,目前还不确定转置一个大矩阵需要多长时间
    
    
    
    val Bv = leftMatrix.rows.context.broadcast(V)
    val vRows = V.numRows
    val vCols = V.numCols
    //leftMatrix * V^T
    var AB = leftMatrix.rows.mapPartitions{iter=>
      val bv = Bv.value
      iter.map { row => 
        val v = BDV.zeros[Double](vRows)
        var i=0
        var j=0
        var dot=0.0
        while (i < vRows) 
        {
          dot=0.0 
          //这样算内积可能会浪费很多时间,因为有些是0.也可能问题不大,因为svd可以将这个维度降到很低
          /*         
          j=0
          while(j < vCols)
          {
            dot+= row(j)* bv(i,j)
            j+=1
          }
          */      
          row.foreachActive((index,value)=>dot+=value*bv(i,index))        
          v(i)=dot
          i+=1
        }
        Vectors.fromBreeze(v)      
         }    
    }
    
    var res =new RowMatrix(AB, leftMatrix.numRows().toInt, vCols)
    
 
    
    //val res = leftMatrix.multiply(rightMatrix)
    
    println("结果:")
    res.rows.collect().foreach(println)
    
    sc.stop()
    
    
    
    
    
    
    
  }
}