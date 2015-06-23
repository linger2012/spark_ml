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
import org.apache.spark.rdd.RDD


object SvdRowMatrix {
  

  
  def runSvd(rows: RDD[Vector],svdNum:Int)=
  {

    
    val mat = new RowMatrix(rows)
    val svd = mat.computeSVD(svdNum,true)//mat.numCols().toInt
     
    val U = svd.U   
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
    
    
    val leftMatrix = new RowMatrix(matrix1)
     
    val V = svd.V

   
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
          row.foreachActive((index,value)=>dot+=value*bv(i,index))        
          v(i)=dot
          i+=1
        }
        Vectors.fromBreeze(v)      
         }    
    }
    
    var res =new RowMatrix(AB, leftMatrix.numRows().toInt, vCols) 
    println("res:")
    res.rows.collect().foreach(println)
    
    
  }
  
  /*
输入格式,完整矩阵
5,5,0,5
5,0,3,4
3,4,0,3
0,0,5,3
5,4,4,5
5,4,5,5
  * */
  def example1()={
    val filePath="/home/linger/dev/spark-1.2.0-bin-hadoop1/data/mllib/svd.data"
    val svdNum=2
  
    val conf = new SparkConf().setAppName("Svd Application")
    val sc = new SparkContext(conf)
      
    val rows = sc.textFile(filePath).map { 
               line => 
               val values = line.split(',').map(_.toDouble)
               Vectors.dense(values)}
       
    
    runSvd(rows,svdNum)

    println("example1 finished")
    sc.stop()
  }
  
/*
输入格式,一行一个向量的数据.每个的格式是  index:value,index:value.0元素不存储
0:5,1:5,3:5
0:5,2:3,3:4
0:3,1:4,3:3
2:5,3:3
0:5,1:4,2:4,3:5
0:5,1:4,2:5,3:5
 */
  def example2={
    val conf = new SparkConf().setAppName("example2 Application")  //
    val sc = new SparkContext(conf)

    // Load and parse the data
    //val data = sc.textFile("/home/linger/dev/spark-1.2.0-bin-hadoop1/data/mllib/sparse_matrix.data")
    val data = sc.textFile("/home/linger/data/movie_data_set.svd_input")
    //val matrix = data.map(_.split(',').map(_.split(':') match{
  //    case Array(index,value)=>(index.toInt,value.toDouble)
  //  }))

    val matrix = data.map{row=>
    var s1= row.split(',').map{one=>
      var s2= one.split(":")match{case Array(index,value)=>(index.toInt,value.toDouble) }
      s2
    }
    s1
    }
    
   matrix.collect.foreach(array=>array.foreach(println)) 

   /*
   val matrixWithSize = matrix.map{array=>
     val maxIndex = array.map(_._1).max+1
     Vectors.sparse(maxIndex,array)
      }
   */
   
   val rowSize = matrix.map{array=>
      array.map(_._1).max+1
   }
   val itemSize = rowSize.max()
   println("itemSize:"+itemSize)
   val matrixWithSize = matrix.map{array=>
     Vectors.sparse(itemSize,array)
      }
    matrixWithSize.collect().foreach(println)

    val svdNum=2
    runSvd(matrixWithSize,svdNum)   
    println("example2 finished")
    sc.stop()
  }
  
  
  def main(args: Array[String]) 
  {
   // println(args(0)) //input 
   // println(args(1)) // 2
    
    //example1()
    example2
 
    
  }
}