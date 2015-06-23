package org.apache.spark.mllib.past.svd

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._ 
import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.mllib.svd.SVDInput
//RDD[IndexedRow]
class SVDModel (rows: IndexedRowMatrix,svdOut:String,svdNum:Int) {
  
  var resMatrix:RDD[(Long, Vector)] = null
  def train(): RDD[(Long, Vector)]=
  {  
   // val mat = new RowMatrix(rows)
    val svd = rows.computeSVD(svdNum,true)//mat.numCols().toInt
    
    val U = svd.U  
    println("U:-------------------------------------------------------------------------")
    U.rows.collect().foreach(println)
    
    val s = svd.s
    println("s:-------------------------------------------------------------------------")
    println(s)
    println("s size:"+s.size)
    
    val sc = U.rows.context
    val Bs = sc.broadcast(s) //将s广播出去,可能广播array更好
    
    
    val UCols = svdNum //U.numCols() 这个值不靠谱阿,她会保留原来矩阵的大小   
    println("step1")
    val matrix1 = U.rows.mapPartitions{iter=>
      val bs = Bs.value
      iter.map { row => 
        val v = BDV.zeros[Double](UCols.toInt)
        val rowIndex=row.index
        val rowVector=row.vector
        rowVector.foreachActive((index,value)=>v(index)=value*bs(index))       
        (rowIndex,Vectors.fromBreeze(v) )     }
    }
    matrix1.foreach(println)    
    
    val leftMatrix = new IndexedRowMatrix(matrix1.map{row=>IndexedRow(row._1,row._2)})
     
    val V = svd.V 
    val Bv = leftMatrix.rows.context.broadcast(V)
    val vRows = V.numRows
    val vCols = V.numCols
    println("step2")
    //leftMatrix * V^T
    var AB = leftMatrix.rows.mapPartitions{iter=>
      val bv = Bv.value
      iter.map { row => 
        val v = BDV.zeros[Double](vRows)
        var i=0
        var j=0
        var dot=0.0
        val rowIndex=row.index
        val rowVector=row.vector
        while (i < vRows) 
        {
          dot=0.0 
          rowVector.foreachActive((index,value)=>dot+=value*bv(i,index))        
          v(i)=dot
          i+=1
        }
        (rowIndex,Vectors.fromBreeze(v))      
         }    
    }   
    
   // val res =new RowMatrix(AB, leftMatrix.numRows().toInt, vCols) 
    println("res:")
   // res.rows.collect().foreach(println)
    
    AB.collect().foreach(println)
    resMatrix = AB
    AB
   //var resMatrix =new IndexedRowMatrix(AB.zipWithIndex().map(one=>IndexedRow(one._2,one._1)))
    //resMatrix = AB.zipWithIndex().map(one=>(one._2,one._1)) 
   // if(svdOut!=null&&svdOut!="") resMatrix.rows.saveAsTextFile(svdOut)    
  }
  

      
    def test(testData:RDD[String]):Double={    //,m: RDD[(Long, Vector)]
    val ratings = testData.map { line => 
      val fields = line.split(',')
      (fields(0).toLong,fields(1).toInt,fields(2).toDouble)
       }
    
   // 记得每一个rdd的map都是分布式执行的.如果map里面访问了其他rdd,则得广播他.否则用join的方法讲相关数据放在一起

     val predictions =  ratings.map(rating=>(rating._1.toLong,rating._2)).join(resMatrix).map{userItems=>
       val user = userItems._1
       val item= userItems._2._1
       val vec = userItems._2._2
       val p = vec.apply(item)
       ((user,item),p)
     }
      
    val pairs = predictions.join(ratings.map(f=>((f._1,f._2),f._3))).values
    math.sqrt(pairs.map(x => (x._1 - x._2) * (x._1 - x._2)).mean()) 
  }

  
}


/*
class SVDInput(sc: SparkContext,ratingFile:String)
{
    val sparseMatrix = sc.textFile(ratingFile).
    map(_.split(',')).map(_.map(_.toDouble)).map(m => (m(0).toLong,m(1).toLong,m(2))).map(k => new MatrixEntry(k._1,k._2,k._3))
    val mat: CoordinateMatrix = new CoordinateMatrix(sparseMatrix)
    //mat.toIndexedRowMatrix().rows.collect().foreach { println} //这时已经打乱了行的顺序
    val rows = mat.toIndexedRowMatrix
    def get = rows
}
*/

object App
{
    
  def main(args : Array[String]) =
  {
       val trainFile="/home/linger/data/movie_data_set.rating_training"
       val testFile="/home/linger/data/movie_data_set.rating_testing"
       
       val conf = new SparkConf().setAppName("SVD Recomendation")  
       var sc = new SparkContext(conf)      
       val svdInput = new SVDInput(sc,trainFile)
       
       val svd = new SVDModel(svdInput.get,"",2)
       svd.train()  
         
       val testData =sc.textFile(testFile)
       val rmse = svd.test(testData)
       println(rmse)
       sc.stop()
  }
}