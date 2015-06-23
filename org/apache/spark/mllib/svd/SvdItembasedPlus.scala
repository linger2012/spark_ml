package org.apache.spark.mllib.svd

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV, axpy => brzAxpy,
  svd => brzSvd,Transpose,pinv,inv}
import breeze.generic.UFunc._
import breeze.linalg.inv.Impl2
import breeze.linalg.inv.Impl
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.linalg.{BLAS}
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.rdd.RDD

import org.apache.spark.SparkContext._ 
import org.apache.spark.broadcast.Broadcast

class SvdItembasedPlus (normMatrix:IndexedRowMatrix,userMeans:Map[Long, Double],svdNum:Int,svdOut:String)
{
 
  var itemSim: CoordinateMatrix=null
  var reducedMatrix:RDD[(Long, Vector)]=null
  def decompose=
  {
            
        
    
        val svd = normMatrix.computeSVD(svdNum,true)//mat.numCols().toInt
        val U = svd.U 
        val s = svd.s
        val V = svd.V 
       
        
        //println("U:-------------------------------------------------------------------------")
        //U.rows.collect().foreach(println)
        
        //println("s:-------------------------------------------------------------------------")
        //println(s)
        //println("s size:"+s.size)
        
       //println("V:-------------------------------------------------------------------------")
        //println(V)
        
       //val t = Matrices.diag(s).toBreeze
        
        //println(V.toBreeze)
        //V.toArray.foreach(println)
 
         
        val VColArray = V.toArray
        
        //V转置并变为RDD
        val vRows=V.numRows
        val vCols=V.numCols
        
        val vtRows=vCols
        val vtCols=vRows
       
        val localVectors =for(i<-0 until vtRows) yield
        {
          val bdv = new BDV(VColArray, i * vRows, 1, vRows)//DenseVector(data: Array[E], offset: Int, stride: Int, length: Int)        
          Vectors.fromBreeze(bdv.map { x => x*math.sqrt(s.apply(i)) })  //
        }
        
        val sc = normMatrix.rows.context 
        val rddVectors = sc.parallelize(localVectors)
        val itemFeatures= new RowMatrix(rddVectors)
        
        itemSim = itemFeatures.columnSimilarities() //感觉余弦距离不是很很靠谱,到时试试其他距离
        //println(itemSim.toBreeze)
        
        
        //还原近似矩阵         
        val Bs = sc.broadcast(s) 
        
        println("step1")
        val UCols = svdNum//很神奇,将svdNum传进下面不行,局部变量UCols反而可以.是不是类成员变量无法传递?
        val matrix1 = U.rows.mapPartitions{iter=>
          val bs = Bs.value
          iter.map { row => 
          val v = BDV.zeros[Double](UCols.toInt)
          val rowIndex=row.index
          val rowVector=row.vector
          rowVector.foreachActive((index,value)=>v(index)=value*bs(index))       
          (rowIndex,Vectors.fromBreeze(v) )     }
        }
        
        
        val leftMatrix = new IndexedRowMatrix(matrix1.map{row=>IndexedRow(row._1,row._2)})
        val Bv = leftMatrix.rows.context.broadcast(V)

        println("step2")
        //leftMatrix * V^T
        reducedMatrix = leftMatrix.rows.mapPartitions{iter=>
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
        
              
  }
  

  
  //需要数据,某个user,所有的评分item,要预测的item(已有item,新来item,目测还没实现对新来的item的预测)
  
  def predict(toPredictPair: RDD[(Long, Int)]):RDD[((Long, Int), Double)]={
 
    
     val red = reducedMatrix.map { row => (row._1,row._2) }
     val userProduct = toPredictPair.join(red)
     val newSim = itemSim.toBreeze.map { x =>0.5+0.5*x }//相似度还需要处理一下,参考这里http://my.oschina.net/zenglingfan/blog/178906 来源于<机器学习实战>
     val sc = reducedMatrix.context
     val globalItemsim = sc.broadcast(newSim)  //之前将broadcast写在decompose函数,运行失败了
     val globalUserMeans = sc.broadcast(userMeans)
     
     val predictions = userProduct.mapPartitions{iter =>
     val simMatrix = globalItemsim.value
     val userMeans = globalUserMeans.value
     iter.map {row=>
       val userId= row._1
       val itemId =row._2._1
       val userRating=row._2._2
       var nu=0.0
       var sum=0.0
       userRating.foreachActive{(index,value)=>              
           val sim = if(index<itemId)
           {
             simMatrix(index,itemId)
           }
           else 
           {
             simMatrix(itemId,index)
           }
         //  println(index,value,sim)
           nu+=sim*(value+userMeans(userId))
           sum+=sim       
       }     
      // println((userId,itemId),nu/sum)
      // println("-------------------------------------------------------")
       ((userId,itemId),nu/sum)   
     }
   }
     predictions
  }
  
  
  
  def test(testData:RDD[String]):Double=
  {
    val ratings = testData.map 
    { line => 
      val fields = line.split(',')
      (fields(0).toLong,fields(1).toInt,fields(2).toDouble)      
    }
    
   val toPredictPair = ratings.map(rating=>(rating._1,rating._2))
   val predictions = predict(toPredictPair)
   val pairs = predictions.join(ratings.map(f=>((f._1,f._2),f._3))).values
   math.sqrt(pairs.map(x => (x._1 - x._2) * (x._1 - x._2)).mean()) 
   
    
  }
  
  
  
}



object SvdItembasedApp
{
    
  def main(args : Array[String]) =
  {
       val trainFile= args(0)// "/home/linger/data/movie_data_set.rating_training"
       val testFile=  args(1)//"/home/linger/data/movie_data_set.rating_testing"
       val svdNum = args(2).toInt//4
       
       val conf = new SparkConf().setAppName("SVD Recomendation")  
       conf.set("spark.executor.memory", "8G")
       var sc = new SparkContext(conf)      
       val svdInput = new SVDInput(sc,trainFile)
       
       
       val svd = new SvdItembasedPlus(svdInput.getNorm,svdInput.rowMean,svdNum,"") //svdInput.getNorm  
       svd.decompose
       
       val testData =sc.textFile(testFile)
       val rmse = svd.test(testData)
       println("rmse:"+rmse)
       
       sc.stop()
    
  }
  
}