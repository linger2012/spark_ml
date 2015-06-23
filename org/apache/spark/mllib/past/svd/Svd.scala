package org.apache.spark.mllib

    
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{BLAS}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.DenseMatrix
object Svd 
{
  def vectorDot(aa:Vector,bb:Vector):Vector=
  {
    var res = new Array[Double](aa.size)
    for(i<-0 to aa.size-1)
    {
      res.update(i, aa(i)*bb(i))
    }
    Vectors.dense(res)
  }
  

  
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
    
    
    
    var matrix1 = U.rows.map{x =>vectorDot(x,s)} 
    var matrix2 = new RowMatrix(matrix1)
    val uRows = matrix2.toBreeze().toArray
    val left = new DenseMatrix(matrix2.numRows().toInt,matrix2.numCols().toInt,uRows)
    println(left)
    println("left")
    println( "rows:"+left.numRows)
    println( "cols:"+left.numCols) 

  
    
    val V = svd.V
    println("V列数"+V.numCols)//2
    println("V行数"+V.numRows)//4
    println("V:-------------------------------------------------------------------------")
    println(V)
   
   /*
   val uRows = U.toBreeze().toArray
   val left = new DenseMatrix(U.numRows().toInt,U.numCols().toInt,uRows)
   println(left)
   println("left")
   println( "rows:"+left.numRows)
   println( "cols:"+left.numCols) 
   */
   
   
   
 
   val right = new DenseMatrix(V.numRows,V.numCols,V.toArray)
   println(right)
   println("right")
   println( "rows:"+right.numRows)
   println( "cols:"+right.numCols)
   
   var res = new DenseMatrix(U.numRows().toInt,V.numRows,new Array[Double](U.numRows().toInt*V.numRows))
   BLAS.gemm(false,true,1.0,left,right,1.0,res)
   

   //val res = left.multiply(right)
   println(res)
   
   
    
    
    sc.stop()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
  }
}