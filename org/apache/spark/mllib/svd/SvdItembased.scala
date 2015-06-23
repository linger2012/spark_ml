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


/*
 * 基于svd的itembased跟传统的itembased的区别在于计算itemsimilarity的方式不同,前者是用降维后的item特征,而后者是原始评分记录作为item特征
 * http://my.oschina.net/zenglingfan/blog/178906
 * http://blog.csdn.net/yuzhongchun/article/details/40779371
 */
class SvdItembased (originMatrix: IndexedRowMatrix,svdNum:Int)
{
  var U:IndexedRowMatrix=null
  var s:Vector=null
  var V:Matrix=null
  var itemSim: CoordinateMatrix=null
  
  def recommend(output:String)=
  {
    if(itemSim == null)
    {
      itemsimilarity
    }
    
    val origin = originMatrix.rows.map { row => (row.index,row.vector) }
    val newSim = itemSim.toBreeze.map { x =>0.5+0.5*x }
    val globalItemsim = originMatrix.rows.context.broadcast(newSim)  
    
    val res = origin.mapPartitions{iter=>
      val simMatrix = globalItemsim.value
      iter.map{row=>
        val userId = row._1
        val ratings = row._2
        val unRatings = ratings.toBreeze.toDenseVector.findAll { x => x==0.0 } //这里可以优化,并不是为0的都要预测.只需要预测跟已有评分item相像的
        val predicts = unRatings.map { itemId=>
           var nu=0.0
           var sum=0.0
           ratings.foreachActive{(index,value)=>              
               val sim = if(index<itemId)
               {
                 simMatrix(index,itemId)
               }
               else 
               {
                 simMatrix(itemId,index)
               }
               nu+=sim*value
               sum+=sim       
           }     
           (itemId,nu/sum) 
        }    
        val sorted = predicts.sortWith{ case ((i1,s1),(i2, s2)) => s1>s2} //此处排序完返回.最后只保留前100个.
        userId+":"+sorted.take(100).mkString(",")
      }
    }
    res.saveAsTextFile(output) 
  }
  
  def itemsimilarity=
  {
        val svd = originMatrix.computeSVD(svdNum,true)//mat.numCols().toInt
        U = svd.U 
        s = svd.s
        V = svd.V 
       
        val sc = U.rows.context
             
        val VColArray = V.toArray
        
        //V转置并变为RDD
        val vRows=V.numRows
        val vCols=V.numCols
        
        val vtRows=vCols
        val vtCols=vRows
        
        val localVectors =for(i<-0 until vtRows) yield
        {
          val bdv = new BDV(VColArray, i * vRows, 1, vRows)//DenseVector(data: Array[E], offset: Int, stride: Int, length: Int)
          Vectors.fromBreeze(bdv)
        }
        
        val rddVectors = sc.parallelize(localVectors)
        val itemFeatures= new RowMatrix(rddVectors)
        
        itemSim = itemFeatures.columnSimilarities()

        
  }
  
  //需要数据,某个user,所有的评分item,要预测的item(已有item,新来item,目测还没实现对新来的item的预测)
  def predict(toPredictPair: RDD[(Long, Int)]):RDD[((Long, Int), Double)]={
     val origin = originMatrix.rows.map { row => (row.index,row.vector) }
     val userProduct = toPredictPair.join(origin)
     val newSim = itemSim.toBreeze.map { x =>0.5+0.5*x }//相似度还需要处理一下,参考这里http://my.oschina.net/zenglingfan/blog/178906 来源于<机器学习实战>
     val globalItemsim = originMatrix.rows.context.broadcast(newSim)  //之前将broadcast写在decompose函数,运行失败了
     
     val predictions = userProduct.mapPartitions{iter =>
     val simMatrix = globalItemsim.value
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
           println(index,value,sim)
           nu+=sim*value
           sum+=sim       
       }     
       ((userId,itemId),nu/sum)   
     }
   }
     predictions
  }
  
  
  def test(ratings: RDD[(Long, Int, Double)]):Double=
  {   
   val toPredictPair = ratings.map(rating=>(rating._1,rating._2))
   val predictions = predict(toPredictPair)
   val pairs = predictions.join(ratings.map(f=>((f._1,f._2),f._3))).values
   math.sqrt(pairs.map(x => (x._1 - x._2) * (x._1 - x._2)).mean()) 
      
  }
  
}



object SvdAPP
{
  
  def recDemo(trainFile:String,output:String,svdNum:Int)=
  {     
       val conf = new SparkConf().setAppName("Itembased Recomendation")  
       conf.set("spark.executor.memory", "16G").set("spark.cores.max","88")
       val sc = new SparkContext(conf)   
       
       val trainData = new SVDInput(sc,trainFile,88) //分区大小影响速度  
      
       val model = new SvdItembased(trainData.get,svdNum)     
       model.recommend(output)   
       
       sc.stop()
  }
  
  def rmseDemo(trainFile:String,testFile:String,svdNum:Int)=
  {
       val conf = new SparkConf().setAppName("SVD Recomendation")  
       conf.set("spark.executor.memory", "16G").set("spark.cores.max","88")
       var sc = new SparkContext(conf)      
       val svdInput = new SVDInput(sc,trainFile)
       
       val svd = new SvdItembased(svdInput.get,svdNum)
       svd.itemsimilarity
       
       val testData =sc.textFile(testFile)
       val ratings = testData.map 
       { line => 
          val fields = line.split(',')
          (fields(0).toLong,fields(1).toInt,fields(2).toDouble)      
       }
       
       val rmse = svd.test(ratings)
       println("rmse:"+rmse)
       sc.stop()
  }
  
  def main(args : Array[String]) =
  {
       val flag = args(0)
       if(flag.equals("test"))
       {
         val trainFile= args(1)// "/home/linger/data/movie_data_set.rating_training"
         val testFile=  args(2)//"/home/linger/data/movie_data_set.rating_testing"
         val svdNum = args(3).toInt//7
         rmseDemo(trainFile,testFile,svdNum)
       }
       else if(flag.equals("rec"))
       {
        val trainFile= args(1)
        val output = args(2)
        val svdNum = args(3).toInt
        recDemo(trainFile,output,svdNum)
       }
       else println("error!")

    
  }
  
}