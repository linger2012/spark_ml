package org.apache.spark.mllib.cf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.SparkContext._ 
import org.apache.spark.broadcast.Broadcast


class Itembased(userItemMatrix: IndexedRowMatrix) {

  val itemSim:CoordinateMatrix= userItemMatrix.toRowMatrix().columnSimilarities()
  
  
  def recommend(output:String)=
  {
    val origin = userItemMatrix.rows.map { row => (row.index,row.vector) }
    val newSim = itemSim.toBreeze.map { x =>0.5+0.5*x }
    val globalItemsim = userItemMatrix.rows.context.broadcast(newSim)  
    
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
  
  
  
  def predict(toPredictPair: RDD[(Long, Int)]):RDD[((Long, Int), Double)]=
  {
    val origin = userItemMatrix.rows.map { row => (row.index,row.vector) }
    val userProduct = toPredictPair.join(origin)
    val newSim = itemSim.toBreeze.map { x =>0.5+0.5*x }//这里已经将分布式的转换为local的了,鉴于item数量较少可以这样做毫无问题
    val globalItemsim = userItemMatrix.rows.context.broadcast(newSim)
    
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
             nu+=sim*value
             sum+=sim       
         }     
         ((userId,itemId),nu/sum)  
      }
    }  
    predictions
  }
  
  
  
  def test(ratings: RDD[(Long, Int, Double)])=
  {
     val toPredictPair = ratings.map(rating=>(rating._1,rating._2))
     val predictions = predict(toPredictPair)
     val pairs = predictions.join(ratings.map(f=>((f._1,f._2),f._3))).values
     math.sqrt(pairs.map(x => (x._1 - x._2) * (x._1 - x._2)).mean()) 
  }
  
}



object ItembasedApp
{
  
  def recDemo(trainFile:String,output:String)=
  {     
       val conf = new SparkConf().setAppName("Itembased Recomendation")  
       conf.set("spark.executor.memory", "16G").set("spark.cores.max","88")
       val sc = new SparkContext(conf)   
       
       val trainData = new RatingInput(sc,trainFile,88) //分区大小影响速度  
       
       
       val model = new Itembased(trainData.get)     
       model.recommend(output)   
       
       sc.stop()
  }
  
  
  def rmseDemo(trainFile:String,testFile:String)=
  {   
       val conf = new SparkConf().setAppName("Itembased Recomendation")  
       conf.set("spark.executor.memory", "16G").set("spark.cores.max","88")
       val sc = new SparkContext(conf) 
       
       val trainData = new RatingInput(sc,trainFile)     
       val model = new Itembased(trainData.get)  
        
        val testData =sc.textFile(testFile)
        val ratings = testData.map{ line => 
          val fields = line.split(',')
          (fields(0).toLong,fields(1).toInt,fields(2).toDouble)      
       }
       
       val rmse = model.test(ratings)
       println("rmse:"+rmse)
       sc.stop()
  }
  
  
  def main(args : Array[String]) =
  {
      val flag = args(0)
      if(flag.equals("rec"))
      {
        val trainFile= args(1)
        val output = args(2)
        recDemo(trainFile,output)
      }
      else if(flag.equals("test"))
      {
         val trainFile= args(1)
         val testFile=  args(2) 
         rmseDemo(trainFile,testFile)
      }
      else 
      {
        println("error!")
      }
       
      
  }

}